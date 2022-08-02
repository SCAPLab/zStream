/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming

import java.io.{File, FileOutputStream}
import java.lang.System.currentTimeMillis

import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.math.BigDecimal

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentBatchTimestamp, CurrentDate, CurrentTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset => OffsetV2, ReadLimit, SparkDataStream, SupportsAdmissionControl}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.v2.{StreamingDataSourceV2Relation, StreamWriterCommitProgress, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.util.Clock

class MicroBatchExecution(
                           sparkSession: SparkSession,
                           name: String,
                           checkpointRoot: String,
                           analyzedPlan: LogicalPlan,
                           sink: Table,
                           trigger: Trigger,
                           triggerClock: Clock,
                           outputMode: OutputMode,
                           extraOptions: Map[String, String],
                           deleteCheckpointOnStop: Boolean)
  extends StreamExecution(
    sparkSession, name, checkpointRoot, analyzedPlan, sink,
    trigger, triggerClock, outputMode, deleteCheckpointOnStop) {

  // lsy start
  // <------------------ Variables of Q-Spark-Extension START--------------------------->
  // * MB = Micro-Batch
  // * DS = Dataset; minimum unit of data which is a collection of raw records. (e.g., single file)
  // * One single MB can contain one or more DSs.
  // Whether to use Q-Spark-Extension or not can be configured.
  // Command: --conf spark.sql.custom.qSpark.enabled=true/false
  var isQSparkRunning = SQLConf.get.enableQSparkExtn
  // Command: --conf spark.sql.custom.qSpark.schedulemode=none/edf/rdas
  var isQSparkScheduleMode = SQLConf.get.scheduleModeQSparkExtn
  // Size of the single current MB.
  var currentMBSize: Double = 0.0
  // Size of total MB during entire query execution.
  var totalMBSize: Double = 0.0
  // Number of datasets (files) within current MB.
  var numDSOfMB: Int = 0 // How many buffer to files during MB?
  // Oldest creation time of a dataset (file) within current MB.
  var oldestDSCreationTimeOfMB: Long = 0
  var lastDSCreationTimeOfMB: Long = 0
  // Total runtime during entire query execution.
  var totalRuntimeOfQuery: Long = 0 // totalRuntime

  var scheduleName: String = sparkSession.conf.get("spark.scheduler.mode")
  // jyw start - Split the result file type
  var resultType: String = "none"
  if ( isQSparkRunning == false ) {
    resultType = "Original-" + scheduleName + "-"
  } else if ( isQSparkRunning == true && isQSparkScheduleMode == "none") {
    resultType = "QSpark-None-"
  } else if ( isQSparkRunning == true && isQSparkScheduleMode == "edf") {
    resultType = "QSpark-EDF-"
  } else if ( isQSparkRunning == true && isQSparkScheduleMode == "rdas") {
    resultType = "QSpark-RDAS-"
  }
  // jyw end

  val resultFileName = "/data/result/" + resultType +
    String.valueOf(System.currentTimeMillis() / 1000) + "-" + name + ".txt"
  val resultFileOut = new FileOutputStream(new File(resultFileName))


  // <------------------ Variables of Q-Spark-Extension END----------------------------->

  // jyw start -- I try to refer QSpark logics`
  val queryName: String = name
  val deadline: Long = extraOptions.get("qspark.deadline").map(_.toLong).getOrElse(0)
  var throughputBuffer = new ListBuffer[Double]()
  var avgThroughputOfQuery: Double = 0.0
  var maxDatasetLatencyOfMB: Double = 0.0
  var minDatasetLatencyOfMB: Double = 0.0
  var numJobCount: Double = 0
  var numJobViolation: Double = 0
  var jobViolationRate: Double = 0.0
  var estimationTime: Double = 0.0
  var tmpEstimationTIme: Int = 0

  // For Debugging(JYW)
  var finalEstimatedRuntime: Double = 0.0
  var finalScheduleTime: Double = 0.0
  var checkTotalBatchSize: Double = 0.0
  var checkThroughput: Double = 0.0
  var checkScheduleTime: Double = 0.0

  // Calculate throughputOfQuery during cold/hot run phases
  var maxThroughputBuffer = new ListBuffer[Double]() // get maxThroughputOfQuery after cold run
  var throughputOfQuery: Double = 0.0 // variables to pass down the FileStreamSource
  var countIter: Int = 0 // count whether the value is bigger than 10
  var countAvg: Int = 0 // for calculating avg
  var total_throughput: Double = 0.0 // for accumulating each throughput buffer value
  // for checking how long each phase takes
  var phaseTime: ListBuffer[Long] = ListBuffer(0.toLong, 0.toLong, 0.toLong, 0.toLong, 0.toLong)

  // scalastyle:off println
  println(s"[Q-Spark: MicroBatchExecution.scala] is Q-Spark (Extension) enabled?: " +
    s"$isQSparkRunning")
  println(s"[Q-Spark: MicroBatchExecution.scala] is Q-Spark Schedule Mode?: " +
    s"$isQSparkScheduleMode")

  Console.withOut(resultFileOut) {
    println("isQSparkRunning",
      "isQSparkScheduleMode",
      "currentMBSize",
      "totalMBSize",
      "totalRuntimeOfQuery",
      "currentMBRuntime",
      "averageThroughputOfQuery",
      "maxDatasetLatencyOfMB",
      "averageLatencyPerQuery",
      "bufferingPhaseTime",
      "constructPhaseTime",
      "schedulePhaseTime",
      "processPhaseTime"
    )
  }
  // scalastyle:on println
  // lsy end

  @volatile protected var sources: Seq[SparkDataStream] = Seq.empty

  private val triggerExecutor = trigger match {
    case t: ProcessingTimeTrigger => ProcessingTimeExecutor(t, triggerClock)
    case OneTimeTrigger => OneTimeExecutor()
    case _ => throw new IllegalStateException(s"Unknown type of trigger: $trigger")
  }

  private var watermarkTracker: WatermarkTracker = _

  override lazy val logicalPlan: LogicalPlan = {
    assert(queryExecutionThread eq Thread.currentThread,
      "logicalPlan must be initialized in QueryExecutionThread " +
        s"but the current thread was ${Thread.currentThread}")
    var nextSourceId = 0L
    val toExecutionRelationMap = MutableMap[StreamingRelation, StreamingExecutionRelation]()
    val v2ToExecutionRelationMap = MutableMap[StreamingRelationV2, StreamingExecutionRelation]()
    val v2ToRelationMap = MutableMap[StreamingRelationV2, StreamingDataSourceV2Relation]()
    // We transform each distinct streaming relation into a StreamingExecutionRelation, keeping a
    // map as we go to ensure each identical relation gets the same StreamingExecutionRelation
    // object. For each microbatch, the StreamingExecutionRelation will be replaced with a logical
    // plan for the data within that batch.
    // Note that we have to use the previous `output` as attributes in StreamingExecutionRelation,
    // since the existing logical plan has already used those attributes. The per-microbatch
    // transformation is responsible for replacing attributes with their final values.

    val disabledSources =
      sparkSession.sqlContext.conf.disabledV2StreamingMicroBatchReaders.split(",")

    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    val _logicalPlan = analyzedPlan.transform {
      case streamingRelation @ StreamingRelation(dataSourceV1, sourceName, output) =>
        toExecutionRelationMap.getOrElseUpdate(streamingRelation, {
          // Materialize source to avoid creating it in every batch
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          val source = dataSourceV1.createSource(metadataPath)
          nextSourceId += 1
          logInfo(s"Using Source [$source] from DataSourceV1 named '$sourceName' [$dataSourceV1]")
          StreamingExecutionRelation(source, output)(sparkSession)
        })

      case s @ StreamingRelationV2(src, srcName, table: SupportsRead, options, output, v1) =>
        val v2Disabled = disabledSources.contains(src.getClass.getCanonicalName)
        if (!v2Disabled && table.supports(TableCapability.MICRO_BATCH_READ)) {
          v2ToRelationMap.getOrElseUpdate(s, {
            // Materialize source to avoid creating it in every batch
            val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
            nextSourceId += 1
            logInfo(s"Reading table [$table] from DataSourceV2 named '$srcName' [$src]")
            // TODO: operator pushdown.
            val scan = table.newScanBuilder(options).build()
            val stream = scan.toMicroBatchStream(metadataPath)
            StreamingDataSourceV2Relation(output, scan, stream)
          })
        } else if (v1.isEmpty) {
          throw new UnsupportedOperationException(
            s"Data source $srcName does not support microbatch processing.")
        } else {
          v2ToExecutionRelationMap.getOrElseUpdate(s, {
            // Materialize source to avoid creating it in every batch
            val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
            val source = v1.get.dataSource.createSource(metadataPath)
            nextSourceId += 1
            logInfo(s"Using Source [$source] from DataSourceV2 named '$srcName' [$src]")
            StreamingExecutionRelation(source, output)(sparkSession)
          })
        }
    }
    sources = _logicalPlan.collect {
      // v1 source
      case s: StreamingExecutionRelation => s.source
      // v2 source
      case r: StreamingDataSourceV2Relation => r.stream
    }
    uniqueSources = sources.distinct.map {
      case source: SupportsAdmissionControl =>
        val limit = source.getDefaultReadLimit
        if (trigger == OneTimeTrigger && limit != ReadLimit.allAvailable()) {
          logWarning(s"The read limit $limit for $source is ignored when Trigger.Once() is used.")
          source -> ReadLimit.allAvailable()
        } else {
          source -> limit
        }
      case other =>
        other -> ReadLimit.allAvailable()
    }.toMap

    // TODO (SPARK-27484): we should add the writing node before the plan is analyzed.
    sink match {
      case s: SupportsWrite =>
        val streamingWrite = createStreamingWrite(s, extraOptions, _logicalPlan)
        WriteToMicroBatchDataSource(streamingWrite, _logicalPlan)

      case _ => _logicalPlan
    }
  }

  /**
   * Signifies whether current batch (i.e. for the batch `currentBatchId`) has been constructed
   * (i.e. written to the offsetLog) and is ready for execution.
   */
  private var isCurrentBatchConstructed = false

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state.set(TERMINATED)
    if (queryExecutionThread.isAlive) {
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
      interruptAndAwaitExecutionThreadTermination()
      // microBatchThread may spawn new jobs, so we need to cancel again to prevent a leak
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
    }
    logInfo(s"Query $prettyIdString was stopped")
  }

  /** Begins recording statistics about query progress for a given trigger. */
  override protected def startTrigger(): Unit = {
    super.startTrigger()
    currentStatus = currentStatus.copy(isTriggerActive = true)
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   */
  protected def runActivatedStream(sparkSessionForStream: SparkSession): Unit = {

    val noDataBatchesEnabled =
      sparkSessionForStream.sessionState.conf.streamingNoDataMicroBatchesEnabled

    var isFinishedTrigger = false // jgh

    // multiQueryInfo("queryName") = name
    // multiQueryInfo("deadline") = deadline
    // multiQueryInfo("margin") = 100

    triggerExecutor.execute(() => {
      if (isActive) {
        // logError("Query name inside trigger: " + name)
        // logError("Multi Queries Info: " + multiQueryInfo.toString)
        var currentBatchHasNewData = false // Whether the current batch had new data
        val mbStartTime: Long = System.currentTimeMillis()
        var jywTempTime: Long = 0
        var jghConstructFinishTime: Long = 0 // jgh need to remove
        var jghConstructStartTime: Long = 0 // jgh need to remove
        var runBatchStartTime: Long = 0 // jgh need to remove
        var runBatchFinishTime: Long = 0 // jgh need to remove
        var scheduleDelay: String = "" // jgh meeasure scheduling delay

//        logError("------------------------------------------------------")
//        logError(s"[$queryName] Deadline: $deadline")
//        logError(s"[$queryName] Start time: $mbStartTime")

        startTrigger()

        reportTimeTaken("triggerExecution") {
          // logError(s"[$queryName] Started by trigger!") // jyw
          phaseTime.update(0, System.currentTimeMillis()) // jgh starting point of buffering phase
          // We'll do this initialization only once every start / restart
          if (currentBatchId < 0) {
            populateStartOffsets(sparkSessionForStream)
            logInfo(s"Stream started from $committedOffsets")
          }

          // Set this before calling constructNextBatch() so any Spark jobs executed by sources
          // while getting new data have the correct description
          sparkSession.sparkContext.setJobDescription(getBatchDescriptionString)

          // Try to construct the next batch. This will return true only if the next batch is
          // ready and runnable. Note that the current batch may be runnable even without
          // new data to process as `constructNextBatch` may decide to run a batch for
          // state cleanup, etc. `isNewDataAvailable` will be updated to reflect whether new data
          // is available or not.
          if (!isCurrentBatchConstructed) {
            jghConstructStartTime = System.currentTimeMillis() // jgh need to remove
            // jgh start
            // for checking how long each phase takes
            phaseTime.update(1, System.currentTimeMillis())
            // jgh end
            isCurrentBatchConstructed = constructNextBatch(noDataBatchesEnabled)
            jghConstructFinishTime = System.currentTimeMillis() // jgh need to remove
            // jgh need toremove
            // logError("constructNextBatch finish Time : " + jghConstructFinishTime)
          }

          // Record the trigger offset range for progress reporting *before* processing the batch
          recordTriggerOffsets(from = committedOffsets, to = availableOffsets)

          // Remember whether the current batch has data or not. This will be required later
          // for bookkeeping after running the batch, when `isNewDataAvailable` will have changed
          // to false as the batch would have already processed the available data.
          currentBatchHasNewData = isNewDataAvailable

          currentStatus = currentStatus.copy(isDataAvailable = isNewDataAvailable)
          if (isCurrentBatchConstructed) {
            if (currentBatchHasNewData) updateStatusMessage("Processing new data")
            else updateStatusMessage("No new data but cleaning up state")

            if (!isFinishedTrigger) {
              // jgh start : Add user-defined QSpark's property to each query
              var qsparkDescriptionOfMB =
                s"""|currentMBSize = Null
                    |totalMBSize = Null
                    |totalRuntimeOfQuery = Null
                    |currentMicroBatchRuntime = Null
                    |avgThroughputOfQuery = Null
                    |maxDatasetLatencyOfMB = Null
                    |deadline = $deadline
                    |oldestDSCreationTimeOfMB = $oldestDSCreationTimeOfMB
                    |queryName = $queryName
                    |numViolation = 0
                    |estimationTime = 0
                    |isQSparkRunning = $isQSparkRunning
                    |isQSparkScheduleMode = $isQSparkScheduleMode
                    |isFinishedTrigger = $isFinishedTrigger""".stripMargin

              sparkSession.sparkContext.setQsparkDescription(getQsparkDescriptionString
                + qsparkDescriptionOfMB)
            }

            // jgh start
            // to initialize query-specific info to properties.
            // info : estimationTime & oldestDSCreationTimeOfMB
            var qsparkCurrentDescriptionOfMB =
              s"""|jobId = Null
                  |oldestDSCreationTimeOfMB = $oldestDSCreationTimeOfMB
                  |estimationTime = $estimationTime""".stripMargin

            sparkSession.sparkContext.setQsparkCurrentDescription(qsparkCurrentDescriptionOfMB)
            scheduleDelay = sparkSession.sparkContext.getOriginalSparkScheduleDelay()
            // jgh end

            runBatchStartTime = System.currentTimeMillis() // jgh need to remove

            // jgh start
            // for checking how long each phase takes
            phaseTime.update(2, System.currentTimeMillis())
            // jgh end

            runBatch(sparkSessionForStream)
            // jgh need to remove
            runBatchFinishTime = System.currentTimeMillis()

            // jgh start
            // for checking how long each phase takes
            phaseTime.update(3, sparkSession.sparkContext.getProcessPhaseTime())
            phaseTime.update(4, System.currentTimeMillis())
          } else {
            updateStatusMessage("Waiting for data to arrive")
          }
        }

        // jgh need to remove
        var ConstructTime = jghConstructFinishTime - jghConstructStartTime
        var runBatchTime = runBatchFinishTime - runBatchStartTime

        // Must be outside reportTimeTaken so it is recorded
        finishTrigger(currentBatchHasNewData, isCurrentBatchConstructed)

        isFinishedTrigger = true // jgh

        // Signal waiting threads. Note this must be after finishTrigger() to ensure all
        // activities (progress generation, etc.) have completed before signaling.
        withProgressLocked { awaitProgressLockCondition.signalAll() }

        val mbEndTime: Long = System.currentTimeMillis()
        val currentMicroBatchRuntime: Long = mbEndTime - mbStartTime

//        logError("*****************************************************")
//        logError(s"[$queryName] End time: $mbEndTime")

        trigger match {
          case t: ProcessingTimeTrigger =>
            if ((t.intervalMs > 0) && (currentMicroBatchRuntime < t.intervalMs)) {
              totalRuntimeOfQuery += t.intervalMs
            }
            else {
              totalRuntimeOfQuery += currentMicroBatchRuntime
            }
        }

//        logError(s"[$queryName] Updated totalRuntime: $totalRuntimeOfQuery")

        if (currentBatchHasNewData) {
          totalMBSize += currentMBSize
          throughputBuffer += (currentMBSize / currentMicroBatchRuntime)

//          if ( runBatchTime > 0 ) {
//            logError(s"[$queryName] JYW Check")
//            logError("onlyRunBatchTime : " + runBatchTime)
//            logError("realRunTime : " + currentMicroBatchRuntime)
//            logError("checkEstimatedTime : " + finalEstimatedRuntime)
//            logError("checkScheduleTime : " + checkScheduleTime)
//            logError("checkTotalBatchSize : " + checkTotalBatchSize)
//            logError("checkThroughput : " + checkThroughput)
//          }

          /* Initialize the cold/hot run variables phase each time */
          countAvg = 0
          countIter = 0
          total_throughput = 0.0
          maxThroughputBuffer.clear()

          /* Implementation throuhputBuffer durning cold/hot phase */
          // Below source has to be optimized later.
          if (scheduleName == "FIFO") {
            if (throughputBuffer.size > 0) {
              if (throughputBuffer.size < 10) { // for cold run phase(10)
                // f.maxThroughputOfQuery = throughputBuffer.last
                throughputOfQuery = throughputBuffer.last
              } else { // After 10, it has to reflect average throughput
                for ( i <- throughputBuffer) {
                  if ( countIter >= 9) {
                    total_throughput += i
                    countAvg += 1
                  }
                  countIter += 1
                }
                throughputOfQuery = total_throughput / countAvg
              }
            }
          } else { // FAIR
            if (throughputBuffer.size > 0) {
              if (throughputBuffer.size < 10) { // for cold run phase(10)
                // f.maxThroughputOfQuery = throughputBuffer.last
                throughputOfQuery = throughputBuffer.last
              } else { // After 10, it has to reflect average throughput
                for ( i <- throughputBuffer) {
                  if ( countIter >= 9) {
                    maxThroughputBuffer += i
                  }
                  countIter += 1
                }
                throughputOfQuery = maxThroughputBuffer.reduceLeft(_ max _)
              }
            }
          }

          avgThroughputOfQuery = totalMBSize / totalRuntimeOfQuery
          maxDatasetLatencyOfMB = mbEndTime - oldestDSCreationTimeOfMB
          minDatasetLatencyOfMB = mbEndTime - lastDSCreationTimeOfMB
          numJobCount = throughputBuffer.length
          if ( maxDatasetLatencyOfMB > deadline ) {
            numJobViolation += 1
          }
          jobViolationRate = BigDecimal(numJobViolation/numJobCount)
            .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
//          logError(s"isQSpark? - [$queryName]'s isQSpark: $isQSparkRunning")
//          logError(s"isScheduleMode? - [$queryName]'s isScheduleMode: $isQSparkScheduleMode")
//          logError(s"MBE - [$queryName]'s endTime: $mbEndTime")
//          logError(s"MBE - [$queryName]'s oldestDSCreationTimeOfMB: $oldestDSCreationTimeOfMB")
//          logError(s"MBE - [$queryName]'s maxDatasetLatencyOfMB: $maxDatasetLatencyOfMB")
//          logError(s"MBE - [$queryName]'s minDatasetLatencyOfMB: $minDatasetLatencyOfMB")
//          logError(s"MBE - [$queryName]'s averageThroughputOfQuery: $avgThroughputOfQuery")
//          logError(s"MBE - [$queryName]'s numJobCount: $numJobCount")
//          logError(s"MBE - [$queryName]'s numJobViolation: $numJobViolation")
//          logError(s"MBE - [$queryName]'s jobViolationRate: $jobViolationRate")
          // Regardless of isQSparkRunning, above code is executed

          // jgh start
          // for checking how long each phase takes
          var bufferingPhase: Long = phaseTime(1) - phaseTime(0)
          var constructMBPhase : Long = phaseTime(2) - phaseTime(1)
          var schedulePhase: Long = phaseTime(3) - phaseTime(2)
          var processPhase: Long = phaseTime(4) - phaseTime(3)

          // scalastyle:off println
          Console.withOut(resultFileOut) {
            println(isQSparkRunning,
              isQSparkScheduleMode,
              new java.math.BigDecimal(currentMBSize),
              new java.math.BigDecimal(totalMBSize),
              totalRuntimeOfQuery,
              currentMicroBatchRuntime,
              avgThroughputOfQuery,
              maxDatasetLatencyOfMB,
              (maxDatasetLatencyOfMB
                + minDatasetLatencyOfMB) / 2,
              bufferingPhase,
              constructMBPhase,
              schedulePhase,
              processPhase
              )
          }
          // scalastyle:on println
          var qsparkDescriptionOfMB = s"""|currentMBSize = $currentMBSize
                                          |totalMBSize = $totalMBSize
                                          |totalRuntimeOfQuery = $totalRuntimeOfQuery
                                          |currentMicroBatchRuntime = $currentMicroBatchRuntime
                                          |avgThroughputOfQuery = $avgThroughputOfQuery
                                          |maxDatasetLatencyOfMB = $maxDatasetLatencyOfMB
                                          |deadline = $deadline
                                          |oldestDSCreationTimeOfMB = $oldestDSCreationTimeOfMB
                                          |queryName = $queryName
                                          |numViolation = 0
                                          |estimationTime = $estimationTime
                                          |isQSparkRunning = $isQSparkRunning
                                          |isQSparkScheduleMode = $isQSparkScheduleMode
                                          |isFinishedTrigger = $isFinishedTrigger""".stripMargin
          sparkSession.sparkContext.setQsparkDescription(getQsparkDescriptionString
            + qsparkDescriptionOfMB)
        }

        // If the current batch has been executed, then increment the batch id and reset flag.
        // Otherwise, there was no data to execute the batch and sleep for some time
        if (isCurrentBatchConstructed) {
          currentBatchId += 1
          isCurrentBatchConstructed = false
        }
        else {
          Thread.sleep(pollingDelayMs)
          totalRuntimeOfQuery += pollingDelayMs
        }
      }
      updateStatusMessage("Waiting for next trigger")
      isActive
    })
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
   *  - currentBatchId
   *  - committedOffsets
   *  - availableOffsets
   *  The basic structure of this method is as follows:
   *
   *  Identify (from the offset log) the offsets used to run the last batch
   *  IF last batch exists THEN
   *    Set the next batch to be executed as the last recovered batch
   *    Check the commit log to see which batch was committed last
   *    IF the last batch was committed THEN
   *      Call getBatch using the last batch start and end offsets
   *      // ^^^^ above line is needed since some sources assume last batch always re-executes
   *      Setup for a new batch i.e., start = last batch end, and identify new end
   *    DONE
   *  ELSE
   *    Identify a brand new batch
   *  DONE
   */
  private def populateStartOffsets(sparkSessionToRunBatches: SparkSession): Unit = {
    sinkCommitProgress = None
    offsetLog.getLatest() match {
      case Some((latestBatchId, nextOffsets)) =>
        /* First assume that we are re-executing the latest known batch
         * in the offset log */
        currentBatchId = latestBatchId
        isCurrentBatchConstructed = true
        availableOffsets = nextOffsets.toStreamProgress(sources)
        /* Initialize committed offsets to a committed batch, which at this
         * is the second latest batch id in the offset log. */
        if (latestBatchId != 0) {
          val secondLatestOffsets = offsetLog.get(latestBatchId - 1).getOrElse {
            throw new IllegalStateException(s"batch ${latestBatchId - 1} doesn't exist")
          }
          committedOffsets = secondLatestOffsets.toStreamProgress(sources)
        }

        // update offset metadata
        nextOffsets.metadata.foreach { metadata =>
          OffsetSeqMetadata.setSessionConf(metadata, sparkSessionToRunBatches.conf)
          offsetSeqMetadata = OffsetSeqMetadata(
            metadata.batchWatermarkMs, metadata.batchTimestampMs, sparkSessionToRunBatches.conf)
          watermarkTracker = WatermarkTracker(sparkSessionToRunBatches.conf)
          watermarkTracker.setWatermark(metadata.batchWatermarkMs)
        }

        /* identify the current batch id: if commit log indicates we successfully processed the
         * latest batch id in the offset log, then we can safely move to the next batch
         * i.e., committedBatchId + 1 */
        commitLog.getLatest() match {
          case Some((latestCommittedBatchId, commitMetadata)) =>
            if (latestBatchId == latestCommittedBatchId) {
              /* The last batch was successfully committed, so we can safely process a
               * new next batch but first:
               * Make a call to getBatch using the offsets from previous batch.
               * because certain sources (e.g., KafkaSource) assume on restart the last
               * batch will be executed before getOffset is called again. */
              availableOffsets.foreach {
                case (source: Source, end: Offset) =>
                  val start = committedOffsets.get(source).map(_.asInstanceOf[Offset])
                  source.getBatch(start, end)
                case nonV1Tuple =>
                // The V2 API does not have the same edge case requiring getBatch to be called
                // here, so we do nothing here.
              }
              currentBatchId = latestCommittedBatchId + 1
              isCurrentBatchConstructed = false
              committedOffsets ++= availableOffsets
              watermarkTracker.setWatermark(
                math.max(watermarkTracker.currentWatermark, commitMetadata.nextBatchWatermarkMs))
            } else if (latestCommittedBatchId < latestBatchId - 1) {
              logWarning(s"Batch completion log latest batch id is " +
                s"${latestCommittedBatchId}, which is not trailing " +
                s"batchid $latestBatchId by one")
            }
          case None => logInfo("no commit log present")
        }
        logInfo(s"Resuming at batch $currentBatchId with committed offsets " +
          s"$committedOffsets and available offsets $availableOffsets")
      case None => // We are starting this stream for the first time.
        logInfo(s"Starting new streaming query.")
        currentBatchId = 0
        watermarkTracker = WatermarkTracker(sparkSessionToRunBatches.conf)
    }
  }

  /**
   * Returns true if there is any new data available to be processed.
   */
  private def isNewDataAvailable: Boolean = {
    availableOffsets.exists {
      case (source, available) =>
        committedOffsets
          .get(source)
          .map(committed => committed != available)
          .getOrElse(true)
    }
  }

  /**
   * Attempts to construct a batch according to:
   *  - Availability of new data
   *  - Need for timeouts and state cleanups in stateful operators
   *
   * Returns true only if the next batch should be executed.
   *
   * Here is the high-level logic on how this constructs the next batch.
   * - Check each source whether new data is available
   * - Updated the query's metadata and check using the last execution whether there is any need
   *   to run another batch (for state clean up, etc.)
   * - If either of the above is true, then construct the next batch by committing to the offset
   *   log that range of offsets that the next batch will process.
   */
  private def constructNextBatch(noDataBatchesEnabled: Boolean): Boolean = withProgressLocked {
    if (isCurrentBatchConstructed) return true

    // lsy start
    // Initialize values that are maintained per MB considering numerous sources per query.
    // currentMBSize = 0.0
    // numDSOfMB = 0
    // lsy end

    // Generate a map from each unique source to the next available offset.
    val latestOffsets: Map[SparkDataStream, Option[OffsetV2]] = uniqueSources.map {
      case (s: SupportsAdmissionControl, limit) =>
        updateStatusMessage(s"Getting offsets from $s")
        reportTimeTaken("latestOffset") {
          val startOffsetOpt = availableOffsets.get(s)
          val startOffset = s match {
            case _: Source =>
              startOffsetOpt.orNull
            case v2: MicroBatchStream =>
              startOffsetOpt.map(offset => v2.deserializeOffset(offset.json))
                .getOrElse(v2.initialOffset())
          }
          // lsy start
          s match {
            case f: FileStreamSource =>
              // jyw start -- [pass variables]
              // queryName, deadline, thoughputOfQuery, or else?
              f.tmpEstimationTime = sparkSession.sparkContext
                .getSpecificQsparkDescription("estimationTime")
              f.queryName = queryName
              f.deadline = deadline
              // jyw start - pass down isQSparkRunning to FileStreamSource
              f.isQSparkRunning = isQSparkRunning
              f.isQSparkScheduleMode = isQSparkScheduleMode
              f.throughputBufSize = throughputBuffer.length

              // jyw end
              if (throughputBuffer.size == 0) {
                f.throughputOfQuery = 0.0
              } else {
                f.throughputOfQuery = throughputOfQuery
              }
          }
          (s, Option(s.latestOffset(startOffset, limit)))
        }
      case (s: Source, _) =>
        updateStatusMessage(s"Getting offsets from $s")
        reportTimeTaken("getOffset") {
          (s, s.getOffset)
        }
      case (s: MicroBatchStream, _) =>
        updateStatusMessage(s"Getting offsets from $s")
        reportTimeTaken("latestOffset") {
          (s, Option(s.latestOffset()))
        }
      case (s, _) =>
        // for some reason, the compiler is unhappy and thinks the match is not exhaustive
        throw new IllegalStateException(s"Unexpected source: $s")
    }
    // Get info after constructing MB.
    uniqueSources.map {
      case (s: SupportsAdmissionControl, limit) =>
        s match {
          case f: FileStreamSource =>
            // Since there might be numerous sources per query, we need to keep add to metrics;
            // instead of assigning new value to them.
            currentMBSize = f.getCurrentMBSize()
            numDSOfMB = f.getNumDSOfMB()
            estimationTime = f.getEstimationTime() // getEstimatedRuntime
            finalEstimatedRuntime = f.getEstimatedRuntime()
            checkTotalBatchSize = f.getCheckTotalBatchSize()
            checkThroughput = f.getCheckThroughput()
            checkScheduleTime = f.getCheckScheduleTime()
            // Since there might be numerous sources per query;
            // we need to update oldestDSCreationTimeOfMB globally.
            var tmpOldestDSCreationTimeOfMB = f.getOldestEventTimestamp()
            // logError("JB tmpOldestDSCreationTime : " + tmpOldestDSCreationTimeOfMB)
            if (oldestDSCreationTimeOfMB == 0 ||
              (tmpOldestDSCreationTimeOfMB != 0 &&
                // jyw start - Modify condition in order to fix "Not Update Time"
                // Prior : oldestDSCreationTimeOfMB > tmpOldestDSCreationTimeOfMB
                (oldestDSCreationTimeOfMB < tmpOldestDSCreationTimeOfMB))) {
              oldestDSCreationTimeOfMB = tmpOldestDSCreationTimeOfMB
            }
            lastDSCreationTimeOfMB = f.getLastEventTimestamp()
            // logError("JB oldestDSCreationTimeOfMB : " + oldestDSCreationTimeOfMB)
          // jyw end
        }
    }
    // lsy end
    availableOffsets ++= latestOffsets.filter { case (_, o) => o.nonEmpty }.mapValues(_.get)

    // Update the query metadata
    offsetSeqMetadata = offsetSeqMetadata.copy(
      batchWatermarkMs = watermarkTracker.currentWatermark,
      batchTimestampMs = triggerClock.getTimeMillis())

    // Check whether next batch should be constructed
    val lastExecutionRequiresAnotherBatch = noDataBatchesEnabled &&
      Option(lastExecution).exists(_.shouldRunAnotherBatch(offsetSeqMetadata))
    val shouldConstructNextBatch = isNewDataAvailable || lastExecutionRequiresAnotherBatch
    logTrace(
      s"noDataBatchesEnabled = $noDataBatchesEnabled, " +
        s"lastExecutionRequiresAnotherBatch = $lastExecutionRequiresAnotherBatch, " +
        s"isNewDataAvailable = $isNewDataAvailable, " +
        s"shouldConstructNextBatch = $shouldConstructNextBatch")

    if (shouldConstructNextBatch) {
      // Commit the next batch offset range to the offset log
      updateStatusMessage("Writing offsets to log")
      reportTimeTaken("walCommit") {
        assert(offsetLog.add(currentBatchId,
          availableOffsets.toOffsetSeq(sources, offsetSeqMetadata)),
          s"Concurrent update to the log. Multiple streaming jobs detected for $currentBatchId")
        logInfo(s"Committed offsets for batch $currentBatchId. " +
          s"Metadata ${offsetSeqMetadata.toString}")

        // NOTE: The following code is correct because runStream() processes exactly one
        // batch at a time. If we add pipeline parallelism (multiple batches in flight at
        // the same time), this cleanup logic will need to change.

        // Now that we've updated the scheduler's persistent checkpoint, it is safe for the
        // sources to discard data from the previous batch.
        if (currentBatchId != 0) {
          val prevBatchOff = offsetLog.get(currentBatchId - 1)
          if (prevBatchOff.isDefined) {
            prevBatchOff.get.toStreamProgress(sources).foreach {
              case (src: Source, off: Offset) => src.commit(off)
              case (stream: MicroBatchStream, off) =>
                stream.commit(stream.deserializeOffset(off.json))
              case (src, _) =>
                throw new IllegalArgumentException(
                  s"Unknown source is found at constructNextBatch: $src")
            }
          } else {
            throw new IllegalStateException(s"batch ${currentBatchId - 1} doesn't exist")
          }
        }

        // It is now safe to discard the metadata beyond the minimum number to retain.
        // Note that purge is exclusive, i.e. it purges everything before the target ID.
        if (minLogEntriesToMaintain < currentBatchId) {
          purge(currentBatchId - minLogEntriesToMaintain)
        }
      }
      noNewData = false
    } else {
      noNewData = true
      awaitProgressLockCondition.signalAll()
    }
    shouldConstructNextBatch
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   * @param sparkSessionToRunBatch Isolated [[SparkSession]] to run this batch with.
   */
  private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    logDebug(s"Running batch $currentBatchId")

    // Request unprocessed data from all sources.
    newData = reportTimeTaken("getBatch") {
      availableOffsets.flatMap {
        case (source: Source, available: Offset)
          if committedOffsets.get(source).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(source).map(_.asInstanceOf[Offset])
          val batch = source.getBatch(current, available)
          assert(batch.isStreaming,
            s"DataFrame returned by getBatch from $source did not have isStreaming=true\n" +
              s"${batch.queryExecution.logical}")
          logDebug(s"Retrieving data from $source: $current -> $available")
          Some(source -> batch.logicalPlan)

        case (stream: MicroBatchStream, available)
          if committedOffsets.get(stream).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(stream).map {
            off => stream.deserializeOffset(off.json)
          }
          val endOffset: OffsetV2 = available match {
            case v1: SerializedOffset => stream.deserializeOffset(v1.json)
            case v2: OffsetV2 => v2
          }
          val startOffset = current.getOrElse(stream.initialOffset)
          logDebug(s"Retrieving data from $stream: $current -> $endOffset")

          // To be compatible with the v1 source, the `newData` is represented as a logical plan,
          // while the `newData` of v2 source is just the start and end offsets. Here we return a
          // fake logical plan to carry the offsets.
          Some(stream -> OffsetHolder(startOffset, endOffset))

        case _ => None
      }
    }

    // Replace sources in the logical plan with data that has arrived since the last batch.
    val newBatchesPlan = logicalPlan transform {
      // For v1 sources.
      case StreamingExecutionRelation(source, output) =>
        newData.get(source).map { dataPlan =>
          val maxFields = SQLConf.get.maxToStringFields
          assert(output.size == dataPlan.output.size,
            s"Invalid batch: ${truncatedString(output, ",", maxFields)} != " +
              s"${truncatedString(dataPlan.output, ",", maxFields)}")

          val aliases = output.zip(dataPlan.output).map { case (to, from) =>
            Alias(from, to.name)(exprId = to.exprId, explicitMetadata = Some(from.metadata))
          }
          Project(aliases, dataPlan)
        }.getOrElse {
          LocalRelation(output, isStreaming = true)
        }

      // For v2 sources.
      case r: StreamingDataSourceV2Relation =>
        newData.get(r.stream).map {
          case OffsetHolder(start, end) =>
            r.copy(startOffset = Some(start), endOffset = Some(end))
        }.getOrElse {
          LocalRelation(r.output, isStreaming = true)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val newAttributePlan = newBatchesPlan transformAllExpressions {
      case ct: CurrentTimestamp =>
        // CurrentTimestamp is not TimeZoneAwareExpression while CurrentBatchTimestamp is.
        // Without TimeZoneId, CurrentBatchTimestamp is unresolved. Here, we use an explicit
        // dummy string to prevent UnresolvedException and to prevent to be used in the future.
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          ct.dataType, Some("Dummy TimeZoneId"))
      case cd: CurrentDate =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          cd.dataType, cd.timeZoneId)
    }

    val triggerLogicalPlan = sink match {
      case _: Sink => newAttributePlan
      case _: SupportsWrite =>
        newAttributePlan.asInstanceOf[WriteToMicroBatchDataSource].createPlan(currentBatchId)
      case _ => throw new IllegalArgumentException(s"unknown sink type for $sink")
    }

    sparkSessionToRunBatch.sparkContext.setLocalProperty(
      MicroBatchExecution.BATCH_ID_KEY, currentBatchId.toString)
    sparkSessionToRunBatch.sparkContext.setLocalProperty(
      StreamExecution.IS_CONTINUOUS_PROCESSING, false.toString)

    reportTimeTaken("queryPlanning") {
      lastExecution = new IncrementalExecution(
        sparkSessionToRunBatch,
        triggerLogicalPlan,
        outputMode,
        checkpointFile("state"),
        id,
        runId,
        currentBatchId,
        offsetSeqMetadata)
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }
    var SupportsWriteStartTime: Long = 0
    var SupportsWriteEndTime: Long = 0

    val nextBatch =
      new Dataset(lastExecution, RowEncoder(lastExecution.analyzed.schema))

    val batchSinkProgress: Option[StreamWriterCommitProgress] = reportTimeTaken("addBatch") {
      SQLExecution.withNewExecutionId(lastExecution) {
        sink match {
          case s: Sink => s.addBatch(currentBatchId, nextBatch)
          case _: SupportsWrite =>
            SupportsWriteStartTime = System.currentTimeMillis()
            // This doesn't accumulate any data - it just forces execution of the microbatch writer.
            nextBatch.collect()
            SupportsWriteEndTime = System.currentTimeMillis()
        }
        lastExecution.executedPlan match {
          case w: WriteToDataSourceV2Exec => w.commitProgress
          case _ => None
        }
      }
    }

    withProgressLocked {
      sinkCommitProgress = batchSinkProgress
      watermarkTracker.updateWatermark(lastExecution.executedPlan)
      commitLog.add(currentBatchId, CommitMetadata(watermarkTracker.currentWatermark))
      committedOffsets ++= availableOffsets
    }
    logDebug(s"Completed batch ${currentBatchId}")
  }

  /** Execute a function while locking the stream from making an progress */
  private[sql] def withProgressLocked[T](f: => T): T = {
    awaitProgressLock.lock()
    try {
      f
    } finally {
      awaitProgressLock.unlock()
    }
  }
}

object MicroBatchExecution {
  val BATCH_ID_KEY = "streaming.sql.batchId"
}

case class OffsetHolder(start: OffsetV2, end: OffsetV2) extends LeafNode {
  override def output: Seq[Attribute] = Nil
}
