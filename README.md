# 6.824 2022-Fall Labs

## 进度

- [x] Lab 1: MapReduce
- [ ] Lab 2: Raft

## Lab1  MapReduce

1. RPC请求只能由worker向coordinator发送；
2. 中间文件格式为`mr-X-Y`，因此设计`MapTaskID`和`ReduceTaskID`；
3. 超时机制使用golang的time.Sleep实现；
4. 根据提示，用`ioutil.TempFile`创建临时文件然后重命名。


## Lab2  Raft

*2A*
1. 需要在论文描述的基础上为Raft struct增加`state`和`electionTimer`两个属性（volatile）；
2. RPC请求需要并行发送，且发生时不能持有`mu`互斥锁（防止死锁）；
3. 心跳包定时器周期选为140ms，选举定时器周期选为300~600ms；