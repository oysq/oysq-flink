## oysq-flink

---

> [官网地址 : https://flink.apache.org/](https://flink.apache.org/)

### 部署

#### 基础架构
![架构](https://nightlies.apache.org/flink/flink-docs-release-1.14/fig/deployment_overview.svg)

1. 总体架构
   * `FlinkClient`
      * 提交任务的客户端
   * `JobManager`
      * 任务调度中心
      * 只有一个
      * 高可用（HA）
   * `TaskManager`
      * 任务执行点
      * 有多个
  
2. 区别和联系：`task` / `slot` / `parallelism`

#### 配置文件
* `conf/flink-conf.yaml`
  * `rest.port`=`webPort`(默认8001)
  * `jobmanager.rpc.address`=`masterIp`
  * `jobmanager.rpc.port`=`rpcPort`
  * `jobmanager.memory.process.size`=`job节点最大内存`
  * `taskmanager.memory.process.size`=`task节点最大内存`(worker节点的配置可以覆盖master节点的配置)
  * `taskmanager.numberOfTaskSlots`=`每个TaskManager的插槽数量`
  * `parallelism.default`=`每个Task的并发数量`
* `conf/masters`
  * `masterIp`:`webPort`
* `conf/workers`
  * `workerIp`

#### 环境变量
`export FLINK_HOME=/usr/local/flink/app/flink-1.13.2`
`export PATH=$FLINK_HOME/bin:$PATH`
`source ~/.bash_profile`

#### 启动 / 停止 flink
* `.$FLINK_HOME/bin/start-cluster.sh`
* `.$FLINK_HOME/bin/stop-cluster.sh`

#### 校验 flink 启动情况
1. `jsp` 是否有 `StandaloneSessionClusterEntrypoint` 和 `TaskManagerRunner` 进程
2. 访问 `localhost`:`webPort`

#### 提交 job
`./bin/flink run -c org.apache.flink.streaming.examples.socket.SocketWindowWordCount ./examples/streaming/SocketWindowWordCount.jar --hostname localhost --port 9527`

`nc -lk 9527`

#### 查看 job
`./bin/flink list`
`./bin/flink list -r`

#### 取消 job
`./bin/flink cancel JobID`

---

### 核心 API

#### StreamExecutionEnvironment

1. 获取上下文
> 什么是上下文？可以理解成一个空间、变量或者工具，初始化的时候可以往里面放各种配置什么的，应用可以使用这个上下文取配置、执行方法等。很多框架都有自己的上下文，比如spring、flink等等。

> 获取上下文的方式：
> 
> 批处理：ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
>
> 流处理：StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
* `getExecutionEnvironment()` 方法会在不同的环境（单机/集群）创建对应的上下文，通常调用该方法即可，不必调用下面两个不同环境时实际执行的方法
  * 单机部署时：`LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();`
  * 集群部署时：`StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment()`


#### Source API

1. 设置数据源
   1. `env.addSource(SourceFunction<OUT> function)`
   2. 几种内置的 Source
      1. File 类型: `readFile()` / `readTextFile()` / ...
      2. Socket 类型: `socketTextStream()` / ...
      3. Collect 类型: `fromCollection()` / `fromElements()` / ...
      4. Custom 类型（自定义类型）: `addSource()` / `addSource(new FlinkKafkaConsumer<>())` / ...


2. 自定义数据源 
   1. 实现方式：实现指定的接口。
   2. 有三种接口可以自定义数据源：
      * `SourceFunction`：不支持并行，即并行度为只能为1
      * `ParallelSourceFunction`：继承`SourceFunction`接口，支持设置并行度
      * `RichParallelSourceFunction`：继承`ParallelSourceFunction`接口，功能最强大

3. 并行度
   1. 全局并行度设置: `env.setParallelism(2);`     // 优先级较低
   2. 算子并行度设置: `filter.setParallelism(4);`  // 优先级较高，默认是系统CPU核心数

#### Transformation API

#### Sink API




