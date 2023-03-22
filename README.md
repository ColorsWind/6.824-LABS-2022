# 6.824 2022-Fall Labs

## 进度

- [x] Lab 1: MapReduce
- [x] Lab 2: Raft
- [x] Lab 3: Fault-tolerant Key/Value Service
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



### Lab3 Fault-tolerant Key/Value Service

*3A*

1. 如果两个初始状态相同的状态机，输入相同Command序列（顺序相同、内容相同），那么两个状态机的末状态仍然相同。在kvraft中，需要用一个`map[string]string`保存当前的状态。考虑要求Clerk的一条CommandGET/PUT/APPEND只会执行一次（即使Clerk请求了两个不同的kvserver），因此一些Command结果也需要作为状态保存、同步。raft已提供Command序列一致性的保证，因此kvraft只需要持续接受applyCh中的信息，更新状态机的状态（包括k-v映射和一些Command的执行结果）；

2. 存在一种情况：Clerk发送请求给leader，leader已经commit但是随后马上fail，reply丢失。在这种情况下，我们允许Clerk发送请求给新的leader，那么同一条Command可能Start两次。

3. 对于Command重复检测（duplicate detection），有三个要求：

   1. Clerk的一次请求只能执行一次，applyCh重复的Command只能执行一次；
   2. 即使Command重复，不执行，Clerk也需要拿到结果；
   3. 重复检测占用的内存可以得到快速释放。

   经过实践，发现给每条Command一个随机，全局唯一的编号有困难。考虑到Hint中提到一个Client在同一时间只会发出一个请求，收到一个请求可以假定上一个请求的reply已经被Client接受（即不会再向kvserver重复请求上一次请求Command的执行结果了），因此一种实现的方式：Client有一个唯一随机编号、每个Command有一个自增id。

4. Hint中提到有两种方式检测raft失去leadership（从而我们可以kvserver可以返回reply：ErrWrongLeader）：检测Start()返回的index和检测GetState()返回的term。实践表明前一种由于死锁方式无法通过`Test: completion after heal (3A)`，因此这里实现了第二种方式。

*3B*

1. 需要正确处理`kill()`，保证所有goroutine在合理的时间内结束，注意各种边界情况
2. 重新设计数据结构：
   1. clientRequest map[client_id] request_handler **volatile**
   2. lastAppliedCommand map[client_id] command **persistent**