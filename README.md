## oysq-flink

---

> [官网地址 : https://flink.apache.org/](https://flink.apache.org/)

### 部署

#### 基础架构
![架构](https://ci.apache.org/projects/flink/flink-docs-release-1.13/fig/deployment_overview.svg)

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
> StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
* `getExecutionEnvironment()` 方法会在不同的环境创建对应的上下文，通常调用该方法即可，不必调用下面两个不同环境时实际执行的方法
  * 单机部署时：`LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();`
  * 集群部署时：`StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment()`


#### Source API

1. 设置数据源
> env.addSource(SourceFunction<OUT> function)
* `SourceFunction`有三种实现
  * `SourceFunction`：不支持并行，即并行度为只能为1
  * `ParallelSourceFunction`：继承`SourceFunction`接口，支持设置并行度
  * `RichParallelSourceFunction`：继承`ParallelSourceFunction`接口，功能最强大

#### Transformation API

#### Sink API




