# 6.824 2022-Fall Labs

## 进度

- [x] Lab 1: MapReduce
- [x] Lab 2: Raft
- [ ] Lab 3: Fault-tolerant Key/Value Service
- [ ] Lab 4: ???

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

*2B*
1. applyCh 需要在单独的goroutine中发送, 为了保证顺序，这样的goroutine只能有一个，可以轮询或者用条件变量控制；
2. Figure 2中 Rules for Servers中的规则可能在任何时候触发，需要在相应的位置检测；
3. RPC请求需要在单独的goroutine中发送；
4. 释放后再获取互斥锁需要重新检查Raft状态，之前的假设可能不满足；

*2C*
1. 根据Hint在AppendEntriesReply中增加XTerm, XIndex, XLen字段，收到reply时通过优化规则更新nextIndex（避免超时）；
2. 需要注意可能收到过时的AppendEntries，需要判断忽略过时的RPC请求；

*2D*
1. 增加LogsHolder，包含属性LastIncludedIndex, LastIncludedTerm，Entries，字段含义与RPC中同名字段相同，且均为persistent，相关数据结构需要仔细考虑，一种可行的想法是：
   - 记录LastIncludedIndex, LastIncludedTerm，可以获取Entries[-1]的Term；
   - 更新Snapshot时候判断LastIncludeedIndex和最新Index的相对位置，决定具体的操作；
   - 准备发送AppendEntries时，如果PrevLog已经discard，先发送InstallSnapshot并更新nextIndex；
   - 准备ApplyMsg时候，如果PrevLog的Term已无法获取，发送Snapshot并更新lastApplied；
2. Snapshot中的信息已提交，不需要检查是否commit；
3. 除了参考论文和Hint的提示外，需要考虑的情况较多，如：
   - 处理Snapshot时，如果LastIncludedIndex > LastLogIndex，需要完全清除日志；
   - 如果收到的AppendEntries的PrevLog已经discard了，但是Entries中包含部分信息Follower中缺少，需要接受这部分较新的消息；
4. Snapshot需要使用Persister中ReadSnapshot和SaveStateAndSnapshot持久化，不能存储到state中。