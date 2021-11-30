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
1. `jps` 是否有 `StandaloneSessionClusterEntrypoint` 和 `TaskManagerRunner` 进程
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

### 并行度（Parallel）

> 一个 Flink 任务由多个节点组成（Source/Transformation/Sink），一个节点由多个并行的实例（线程）共同执行，这些实例（线程）的数量就是这个节点的并行度。

#### 并行度的设置（从上往下优先级依次降低）

1. `Operator Level`：在 `Source`/`Transformation`/`Sink` 上使用 `setParallelism()` 方法设置
2. `Execution Environment Level`：在 `ExecutionEnvironment` 对象上使用 `setParallelism()` 方法，他将作用于上下文的所有 `Operator`
3. `Client Level`：在将任务提交到 `Flink` 的时候设置，比如 `./bin/flink run -p 10 job.jar`
4. `System Level`：可以通过配置文件 `flink-conf.yml` 的 `parallelism.default` 属性来指定所有执行环境的默认并行度

#### TaskManager、slot和并行度之间的关系

1. `TaskManager` 的数量是通过 `jps` 命令看到的进程的数量
2. 若 `taskManager` 数量是3个，且 `flink-conf.yml` 文件配置的 `taskmanager.numberOfTaskSlots = 3`, 则系统总 slot 数量为9个
3. 若 `Source` 并行度3，则它占用3个slot，同理 `Transformation` 并行度7占用7个slot，`Sink` 并行度5占用5个slot，但总共占用7个slot，因为一个slot可以同时处理1个 `Source` + 1个`Transformation` + 1个`Sink`
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
   3. 常见套路：
      * 在 `open()` 获取链接（每个实例只执行一次，避免每次下沉都重新获取链接）
      * 在 `close()` 关闭链接（每个实例只执行一次，避免重复关闭）
      * 在 `invoke()` 执行数据下沉（每条数据执行一次）

#### Transformation API

1. 常见算子
   * `filter()`：过滤满足条件的数据
   * `map()`：作用在每一个元素上，进来是多少个，出去就是多少个 
   * `flatMap()`：作用在每个元素上，一进多出，这个多也可能是一个或没有 
   * `keyBy()`: 按字段分组（如果是用对象多某个字段，对象必须有无参构造器） 
   * `reduce()`：将分组后，相同key的数据放到同一个task进行操作，入参是上一个迭代完的数据和这一条新进来的数据 
   * `sum()`：聚合函数，用于分组后的常见简单操作
   * `union()`: 多个 stream 合并为一个stream，每个流的数据结构要求相同，也可以自己和自己合并，将会处理多次自己，后面也是两个流走同一个 Transformation 算子
   * `connect()`: 两个 stream 合并为一个stream，两个流的数据结构可以不同，返回类型为 ConnectedStreams，需要借助 CoMap(一进一出) 或 CoFlatMap(一进多出) 才能转为 DataStream，且每个流有自己的 Transformation 算子

2. 分区器
   1. 作用：分区策略决定的是一条数据要分给自己上游算子的哪个分区
   2. 八大分区策略（分区器）：即抽象类 `StreamPartitioner` 的八个实现
      * `GlobalPartitioner`：都给第一个 `operator`
      * `ShufflePartitioner`：随机发给某一个 `operator`
      * `RebalancePartitioner`：循环分发给 `operator`
      * `RescalePartitioner`：循环分发给 `operator`，但是会基于自己的并行度，和上游的并行度。例如自己并行度是2，上游是4，则自己的第一个实例循环给上游的某两个实例，自己的另一个实例循环给上游的另外两个实例。若自己并行度是4，上游并行度是2，则自己的某两个实例循环给上游的某一个实例。
      * `BroadcastPartitioner`：广播分区，每个上游实例都分一份自己的数据，适合每个实例的大数据都需要join同一份小数据的场景，比如性别。
      * `ForwardPartitioner`：分发给本地的`operator`,这个分区器要求上下游的并行度一样，且上下游的算子在同一个`task`内（即为本地）
      * `KeyGroupStreamPartitioner`：将记录按`key`的`hash`值分发到某个固定实例
      * `CustomPartitionerWrapper`：使用自定义分区器，传入实现了`Partitioner<K>`接口的实例
   3. 自定义分区器
      * 方式：实现 `Partitioner<K>` 接口，并调用 `partitionCustom()` 方法传入该实现类的实例
   4. 注意点：
      * 分区策略只是指定分配数据给上游的规则，不会改变上游的分区数量
      * 上游算子的分区数设置不能小于分区器需要的数量
      * 只会影响跟在后面的第一个算子，再往后的算子与此无关

#### Sink API

1. 内置 Sink
2. 自定义 Sink

#### 注意点

* lambda 表达式在 javac 编译后会使泛型丢失，需要使用 returns() 方法指定返回值类型，所以虽然可以使得算子的代码更简洁，但最好还是用匿名类或者自定义UDF实现类的方式

---

### Windows

> Window 是对数据分段处理的方式，每一段称之为一个窗口

#### 时间语义

1. EventTime（Flink 1.12 版本开始，成为默认语义）
   * 说明：使用数据产生的真实时刻作为统计分析的基准点（比如操作日志，则指用户点击界面产生日志的时刻会记录在数据里传递过来，Flink 再提取这个字段作为基准点）
   * 特性：
      * 性能低、延时高：因为需要等待指定时间段内的数据到达 Flink
      * 确定性100%：执行结果肯定是确定的，可以解决乱序、延迟数据的问题
   * 场景：适用于对顺序、时间敏感的场景。比如"关注、取消关注、关注..."这样的数据按不同的顺序到达，最后的结论是不同的，或者是需要按时间段统计的场景
   
2. IngestionTime
   * 说明：使用数据进入 Flink 系统的时刻（这个新版 Flink 的说明图内已经没有提及了）

3. ProcessingTIme
   * 说明：使用 Flink 处理这条数据时，当前系统的时刻作为统计分析的基准点
   * 特性：
      * 性能高、延时低：数据能尽快处理，因为不关心数据实际发生的时间，只关心当前 Flink 系统的时间
      * 不确定性高：数据从产生到被算子处理，会经过队列、网络、其他算子的处理等等，耗时都是不确定的
   * 场景：适用于对时间不敏感的场景，比如转发数据到数据库、每条数据单独处理、互不关联的场景
     
4. 简单举个例子：
> 比如有如下数据：
> 
> --> "关注了你 13:01"
> 
> --> "取消关注 13:02"
> 
> --> "关注了你 13:03"
> 
> --> "取消关注 13:04"
> 
> 如果 Flink 按 EventTime 处理，最后结果将肯定是：没有关注你
> 
> 如果 Flink 按 ProcessingTIme 处理，最后结果可能会因为乱序问题，每次都产生不一样的结论

#### Window 的各种分类

1. 是否KeyBy分类
    * stream.windowAll(...)
    * stream.keyBy().window(...) // 如果这边用了 windowAll()，则会把所有的key都视为第一个收到的key来处理，并不会报错

2. 划分方式分类
   * 按时间划分（Time-based Window）：按指定的时间长度(区间左闭右开：[0, 5) -> [5,10) -> ...)划分窗口（同一个窗口的时间长度相同，与窗口内的 event 数量无关）
   * 按数量划分（Count-based Window）：按指定的数量划分窗口（同一个窗口的 event 数量相同，与窗口的时间长度无关）
   
3. 窗口分配器分类
   * 作用：窗口分配器的职责是将进来的元素分配到一个或多个窗口中
   * 内置窗口分配器：
      * 滚动窗口 tumbling windows
         * 窗口是对齐的，不会有重叠，一个 event 只会划分到一个窗口
         * 窗口的划分方式可以按时间或者数量，时间又可以分为按 EventTime 或 ProcessingTime
         * 使用时仅需要设置窗口的大小
      * 滑动窗口 sliding windows
         * 窗口是滑动的，可能会有重叠，一个 event 可能会被分配到零个、一个或多个窗口
         * 窗口的划分方式可以按时间或者数量，时间又可以分为按 EventTime 或 ProcessingTime（和滚动窗口相同）
         * 使用时需要设置窗口大小（window size）+滑动步长（window slide），当步长小于大小时，会有部分 event 被一个或多个窗口捕捉，当步长等于大小时，效果就等同于滚动窗口，当步长大于大小时，将有部分 event 不会被捕捉到
      * 会话窗口 session windows
        * 窗口是不连贯的，不会有重叠。一个 event 只会被划分到一个窗口
        * 在指定间隔（session gap）后，还是没有收到新的 event，将认为一个会话结束了，并关闭当前窗口
        * 使用时需要指定一个间隔（session gap）
      * 全局窗口 global windows
        * 该窗口几乎不会用到
   * 自定义窗口分配器
      * 继承实现 WindowAssigner 类

#### 生命周期

1. 窗口的创建：在收到第一条数据的时候会创建第一个窗口，后续的每个窗口根据分配器规则创建
2. 窗口的销毁：在 Time-base Window 类型的窗口，每个窗口在创建后，会在指定的时间长度，再加上允许等待时间长度后，才销毁

#### WindowFunction

1. WindowFunction 指在数据进入各自的窗口之后，要对窗口内的数据进行处理的方法
2. 种类
   * ReduceFunction：增量处理，一条数据执行一次
   * AggregateFunction：增量处理，一条数据执行一次，相比 ReduceFunction 更为通用，可自定义程度更高
   * ProcessWindowFunction：全量处理，等窗口内数据都到达后，统一处理一次

#### Watermark（水印）

> Watermark 是一种延时等待策略，来自 Google 的 DataFlow 模型，是一种衡量 Event 进展的机制

1. Watermark 将标记窗口的当前时间为 context.currentWatermark() = 当前数据接收到的最大时间 - 延时等待的时间（规定值）
2. 通过 `assignTimestampsAndWatermarks()` 方法指定事件时间（EventTime）字段，并设置一定的延时等待时间，等待时间一到，就触发窗口处理方法，然后销毁窗口
3. 若数据在等待时间后（窗口已经销毁）才到达，则需要通过 `sideOutputLateData()` 方法收集`测流输出`的数据，最后通过 `getSideOutput()` 方法对超过等待时间的数据进行再次处理

### State

> 状态是 Flink 的一个强项，它可以支持有状态的数据处理。State 内存储了当前运行的一些信息，可以自行对它增删改查，比如：当前求和累加到了多少美元、当前已经读取的学生数量等等
> 
> 并不是只有自定义的时候才会用到 State，Flink很多方法的内部实现用的也是 State，比如 sum() 等 

* 有状态：每个批次的数据都是基于上一个批次的数据基础上进行的
* 无状态：每个批次之间没有任何关联

#### 状态的分类

* Keyed State
   * ValueState
   * ListState
   * ReducingState
   * AggregatingState
   * MapState
* Operator State
* Broadcast State

#### CheckPoint

> CheckPoint：检查点，指 Flink 的容错恢复机制，是 Flink 高可用的基石，也是所谓的状态管理；

1. 前置要求
   * 接入数据的数据源需要支持一定时间范围内的回放，因为异常时，数据可能是需要重新读取的（例如：Kafka 可以通过 offset 来支持一段时间内的数据回放，默认是7天）
   * State 必须持久化到一个可靠的文件存储系统内（例如：HDFS、NFS等等）

2. 开启方式
   * CheckPoint 默认为关闭
   * 在 StreamExecutionEnvironment 内调用 enableCheckpointing(n) 来开启，n 为持久化周期

3. 配置项
   * 持久化周期：多久对 CheckPoint 做一次持久化，单位为毫秒
   * 两次持久化之间的最小时间：例如设置为5秒，则下一次持久化一定要等到本次持久化结束5秒后才会进行（为防止持久化周期过短或持久化耗时过长，导致同时存在两个持久化操作）
   * 超时时间：对 CheckPoint 做持久化到存储时，如果耗时超过这个时间，会中断本次持久化操作
   * 消费模式：包含精准消费一次（CheckpointingMode.EXACTLY_ONCE）、最少消费一次（CheckpointingMode.AT_LEAST_ONCE）两种模式
   * 持久化的并行数量：默认情况下，并行度是1（同一时间只能有一个持久化在进行）
   * 外部的存储：可以配置一个外部存储，把 CheckPoint 存储到外部系统，这样当 Job 挂掉时，可以从外部存储系统来恢复状态
   * CheckPoint对Task的影响：可以配置当 CheckPoint 失败时，Task 是否也会挂掉，还是会把消息发送到某个地方，默认是会一起挂掉
   * 是否优先从 CheckPoint 恢复 Task：默认为是

4. 重启策略
   * 重启策略可以在 `flink-conf.yml` 文件内配置，也可以在代码内对 `env` 进行配置，后者优先级更高，会覆盖前者
   * 重启策略在开启 `CheckPoint` 之后才会生效，默认的重启策略是重试 `Integer.MAX_VALUE` 次，如果还是不行，就结束退出
   * 重启次数指的是整个运行期间累计可以重启的次数，而不是某次故障可以重启的次数，即：每次故障都会消耗掉部分次数余额

5. State Backend
   1. Backend 是 CheckPoint 内对 State 定时备份的机制，默认是关闭的
   2. 配置方式：
      * 在 `flink-conf.yml` 里配置 `fs.default-scheme: file:///root/app/flink/checkpoints`，这是推荐的做法，因为每个环境的存储配置都不一样
      * `env.setStateBackend(...)`
      * 可以存储在：JobManager memory, file system, database 三种地方
   3. 内置的三种 Backend 方式（ootb: Out of the box）
      1. MemoryStateBackend（默认配置）
         1. 内存的方式适用于开发，因为对 State 的大小是有限制的，默认是限制5M，可以修改，但是不能超过akka的大小
         2. 可以设置同步或者异步的方式，同步会阻塞，所以推荐使用异步，默认的配置也是异步
         3. State 存储在 TaskManager 的内存中，CheckPoint 存储在 JobManager 的内存中
         4. Task 挂了会从 JobManager 那边恢复 State，而整个 Flink 挂了，就找不回来了（根本原因是 jvm 挂了，内存清空了）
         5. 故障时从内存读取 State 是默认的行为，不需要配置
      2. FsStateBackend
         1. 可以支持大窗口和大 State，适合于生产环境
         2. 故障时，默认还是会去内存里读取上次状态进行恢复，需要配置才会去读取 FsStateBackend
      3. RocksDBStateBackend
         1. 可以支持大窗口和大 State，适合于生产环境
            1. 故障时，默认还是会去内存里读取上次状态进行恢复，需要配置才会去读取 RocksDBStateBackend

6. 完整使用方式：
   1. 开启 CheckPoint 并配置故障重启次数，这样故障后会自动从内存读取状态恢复 State
   2. 配置 Backend 并配置 ExternalizedCheckpoints 为故障不删除
   3. 在整个 jvm 都挂了（达到重启次数/cancel job）之后，运行 jar 包时指定从哪个地方（fileSystem/database）的 Backend 恢复 State

7. 注意点：
   * 当配置了 Backend 的方式为文件系统或数据库时，State 还是会以内存的方式再存储一份，用于故障自动重启，所以说自动重启的时候并不会去读取文件系统或数据库
   * 而文件系统或数据库的那一份存储用于整个任务都挂了之后，手动启动时，指定恢复位置









