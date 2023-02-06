# 6.824 2022-Fall Labs

## 进度

- [x] Lab 1: MapReduce
- [ ] Lab 2: Raft

## Lab1  MapReduce

1. RPC请求只能由worker向coordinator发送；
2. 中间文件格式为`mr-X-Y`，因此设计`MapTaskID`和`ReduceTaskID`；
3. 超时机制使用golang的time.Sleep实现；
4. 根据提示，用`ioutil.TempFile`创建临时文件然后重命名。