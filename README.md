zStream(Towards a Low Latency Micro-Batch Streaming System)
=============
# zStream Goal
- The main idea of zStream’s design is to use a reference value called deadline for each query to constrain latency around a specific time. Considering the deadline information of each query, zStream controls the admission of every micro-batch dynamically and schedules them by their priorities, significantly reducing the average and tail latency while maintaining throughput.
  - Dynamic-batching and prioirty task scheduling mechanism(zStream) was built on Spark Structured Streaming(3.0.1) and this mechaism was made with Sogang DCCLAB(Distributed Cloud Computing Lab).

## zStream Paper
- S. Lee, Y. Jeong, M. Kim and S. Park, "Q-Spark: QoS Aware Micro-batch Stream Processing System Using Spark," 2021 IEEE International Conference on Autonomic Computing and Self-Organizing Systems Companion (ACSOS-C), 2021, pp. 38-43, doi: 10.1109/ACSOS-C52956.2021.00027.
- Cluster Computing Journal 22(Currently revisioning..)

### zStream Benchmark
- https://www.vldb.org/conf/2004/RS12P1.PDF
- github.com/akssus12/linear-road-streaming-benchmark

