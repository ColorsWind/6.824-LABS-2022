# 6.824 2022-Fall Labs

## 进度

- [x] Lab 1: MapReduce
- [x] Lab 2: Raft
- [x] Lab 3: Fault-tolerant Key/Value Service
- [x] Lab 4: Sharded Key/Value Service (+ challenge exercises)

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
   - clientRequest map[client_id] request_handler **volatile**
   
   - lastAppliedCommand map[client_id] command **persistent**



**Lab 4: Sharded Key/Value Service**

*4A*

1. 代码框架和kvraft基本相同，且需要实现kvraft的快照部分；

1. 要求在`Join`和`Leave`后重新平衡Shard的分配，需要实现一个分配算法，一种可能的实现是：

   ```go
   type GroupItem struct {
   	gid    int
   	shards []int
   }
   type GroupItemList []GroupItem
   
   func reBalance(items GroupItemList) (balancedItems GroupItemList)
   func shardToGroupItemList(shards [NShards]int, gidList []int) GroupItemList
   func groupItemListToShard(gil GroupItemList) (shards [NShards]int, groupN int)
   ```

   需要考虑两个细节：

   - gid=1意味着shard没有被分配给Group；
   - 多于NShards个Group时，Shards中仅会出现这些Group的一部分；

2. 平衡Shard的算法必须是一个确定性（deterministic）的算法。一个不确定的可能来源是对map的遍历，可以先排序再遍历；

3. 编写单元测试是一个好习惯，在单元测试中可以设定伪随机数seed，多次重复生成（生成的对象内存地址不同，但是相等），测试算法的确定性。

*4B*

1. （TestStaticShards）需要仔细考虑哪些部分属于状态机的状态（state），为了确保一致性，对Shard所有权的检查，ReConfiguration生效的时间点的确定应该在`onApplyChMsg`中进行；

2. （TestJoinLeave）为了避免两个Group交换数据时可能产生死锁，可以按照配置文件生效的情况划分shardkv的状态，例如：

   - Sn：编号为n的Configuration已生效，可以处理GET/PUT/APPEND请求（需检查Group），接受一个ReConfiguring请求可以转化为C(n+1)；
   - C(n+1)：编号为(n+1)的Configuration生效中，拒绝所有GET/PUT/APPEND请求，可以一个ReConfigured请求转化为S(n+1)；
   - 在实现过程中，可以新增两个状态ConfiguredConfig和PreConfig记录两个Config实现；

3. （TestConcurrent1,2,3）需要定期检查Configuration，一种可能的实现是：

   - Query(-1)获取最新的Configuration；
   - 通过ReConfiguring是否成功，以及返回值“猜测”shardkv当前的ConfiguredConfig和PreConfig实现；
   - 如果得到了新的shard的所有权，GetState获取；
   - 发送ReConfigured（附上GetState结果）；

   需要考虑的因素包括：

   - 为了保证能找到正确的Group，且在ReConfiguration的过程中不丢失信息，应当严格按照(2)的状态转移，只允许shardkv中的Configuration编号每次增加1，由于存在GetState的依赖关系，我们实现了一个LockStep的系统，两个Group中的Configuration编号不会相差太远，这样可以简化我们GetState的实现；
   - 如果ReConfiguration调用频繁，shardkv不一定能接受Query(-1)的Configuration（查询结果太新），这时候要根据返回值猜测shardkv当前的CurrConfiguration并尝试CurrConfiguration + 1；
   - 仅在`onApplyChMsg`可以保证原子性，在其他函数中任意两条语句中都有可能发送ReConfigure，相关逻辑应当全面考虑这些情况。ReConfigure的相关op必须是幂等的，如果op失败，返回false；如果操作成功，或者允许op但是op已经执行不需要重复执行也返回true；
   - 在shardctrler和raftkv中，均使用全局唯一ClientId+自增CommandId实现GET/PUT/APPEND的duplicate检测，但对于ReConfigure相关op应当使用ConfigNum来检测；需要独立的逻辑不能复用前述逻辑（例如使用ConfigNum生成CommandId），原因包括：ReConfigure相关op可能由多个Client调用，ReConfigure相关op可能执行失败；
   - 考虑上述所有因素，pollConfiguration相关实现可能会较复杂，应当从server.go中独立出来，在我的实现中相关代码在configure_client.go中；

4. （TestChallenge1Delete）当G1失去shard=x的所有权时，在[NShards]State数组中存储shard的。增加一种RPC类型GetStateOK，执行顺序为：G1向G2请求GetState，然后G2提交ReConfigured成功；然后G2向G1发送GetStateOK，然后G1删除相应状态。其中的一些细节：

   - GetState和ReConfigured需要确保执行成功，GetStateOK不需要保证成功（事实上也没办法保证执行成功，有G2内的所有shardkv都可以执行上述过程）；
   - 如果G1再次失去shard=x的所有权时，可以直接覆盖State[x]（如果G1可以失去shard=x的所有权，G1必然从另一个Group获取shard=x的所有权，因此State[x]可以确保已被确认收到；

5. （TestChallenge2Unaffected）在shardkv中使用[shardctrler.NShards]bool存储AffectShards，在(2)中的检查中排除即可；

6. （TestChallenge2Partial）复用(5)中的AffectShards，将一个ReConfigured拆分为多个，当AffectShards全为false时状态Sn -> Cn，其中一些细节：

   - GetState必须并行发送，避免一个Group的故障影响对其他Group的GetState的执行；
   - 在configure_client.go中需要维护多个ClientId和CommandId，必须保证一个ClientId同一时刻只能发送一个RPC请求。