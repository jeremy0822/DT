## 简介

Flink与Spark Streaming是现在流行的两大实时计算引擎，都是Apache基金会的顶级项目。

## 编程模型

分布式计算框架一般都是基于DAG（有向无环图）编程，Flink与Spark Streaming也是。Flink和Spark Streaming都将操作定义为算子（Operator），用户通过实现这些算子的接口定义自己的逻辑，并定义这些算子之间的路由关系，就组成了一个DAG图，一个示例如下。

![](res/DAG.png)

在实时计算中有两个特殊的Operator：Source和Sink。Source负责从数据源拉数据，Sink负责往外部写计算结果。用户实现一些算子定义自己的逻辑，将从Source读到数据的进行处理，并将结果通过Sink写到外部系统，在其他算子中也可以访问外部系统

## 执行调度模型

* **带并行度的DAG图**

从编程模型上看Flink与Spark Streaming都是采用的DAG模型，在运行时都支持Operator上的并行度，加上并行度的DAG图，示例如下。

![](res/带并行度的DAG.png)

Flink与Spark都支持Operator上的并行度，所以理论上吞吐是没有上限的，同时它们都使用了将操作符分组的优化，比如图2中的Source和Map形成一个chain，一个chain内的operator会被部署到同一台机器上。在一台机器上的Source会发送到同一台机器上的Map，这样避免了不必要的网络传输，提供了执行效率。

* **Spark Streaming的执行调度模型**

Spark是一个批处理计算引擎，Spark Streaming是Spark的一个上层应用，运用时间段对数据切分，然后将切分后的数据，交给Spark处理。

![](res/单条数据聚合成小批次.png)

Spark Streaming负责将从Source读到的一条条的数据按时间段组成小批次，小批次的数据交给Spark 当成批处理计算。

![](res/小批次数据交给SparkEngine处理.png)

在Job执行期间来说，用的依然是批处理的调度机制, 针对图2中的DAG图解释如下，对一个批次来说，所有的节点都计算source和map，完成了之后再计算Group By，最后再是Sink，由任务调度器不断的给节点分配任务。
所以处理延时可以用如下表示：

**处理延时 = 处理时间 + 调度时间 + 网络传输时间 + Batch周期**

因为在每个批次，Mater中的调度器要根据任务执行情况，再分配新任务，Master是个单点，在数据量特别大时，容易成为瓶颈，qps在10W以上时特别明显。

* **Flink的执行调度模型**

Flink在Job启动时，调度器会将Operator部署到各个机器上，之后不会再调度任务的部署了，所以任务的执行视图，如下。

![](res/flink调度模型.png)

Job启动后，Source接收的数据会经过各个Operator的处理最后到Sink输出到外部系统，不存任务的调度。Flink的处理是基于单条记录的，每个Operator处理完一条记录后，就传给下一个Operator。

**处理延时 = 处理时间 + 网络传输时间**

## 对比

* **延迟**

从调度执行模型上看，Flink的延迟更低，可以到毫秒级；Spark Streaming的延迟是秒级，公司上线的Spark Streaming业务一般在10s以上。

* **数据分割**

Flink与Spark Streaming都支持Operator上的并行，所以都支持对从数据源接收到的数据分割，进而并行处理。但Flink在Job正式运行时，每个Operator被部署的机器是不变的，所以可以做热数据集的分割，如一个Job中的一个Operator会访问外部一个40G的数据，可以将此Operator的并行度设为10，进而每个Operator的并行度加载4G的数据，然后控制此Operator的父Operator的路由，就可以让上游的数据路由到加载了对应热数据集的Operator上去，加速处理。Spark Streaming由于是任务的部署是变化的，只能每个Operator加载全部的数据。

* **与外部存储的交互**

Flink与Spark Streaming几乎都能使用常见的外部存储，DB，Mango，Redis，HBase，Elastic Search，但对外部系统的压力是不同的，Spark Streaming先将数据聚合成小Batch再访问外部系统，会造成数据短暂的聚集，造成短时间内对外部存储太大压力，如Spark Streaming聚集Batch的周期是1分钟，处理时间是20s，对于外部存储来说QPS升了3倍，从外部存储看，QPS一会处于比较高的水平，一会又变为0。Flink因为是基于单条数据处理的，所以QPS是平滑的。

* **特有功能**

**Flink的特有功能：**

   * 支持多种窗口。基于计数的窗口（如100条记录一个窗口），基于时间的窗口（5分钟一个窗口），基于Session的窗口（根据会话的时间设置窗口），并支持自定义窗口。
   * 支持真实时间。支持根据数据本身带的时间戳进行计算，可以使统计分析更准确。
   * 支持CEP（Complex Event Processing，复合事件处理），可以找到事件之间的模式。
   * 支持启动与关闭。Flink的算子可以除了实现处理逻辑，还有如下两个接口。
   * open会在Job启动时执行，一般做些初始化的工作，或者加载热数据；close会在Job停止时，调用做些善后工作。
      * @Override
public void open(Configuration parameters) throws Exception {
    //Job启动时调用一次，做些初始化的工作，例如热数据加载，建立数据库连接
}
@Override
public void close() throws Exception {
    //Job停止时调用一次，做些善后工作
}

**Spark Streaming特有功能：**

相比于Flink这种专门的流处理引擎没有特有功能。

* **选型**

从底层来看Flink是真正的面向实时的处理引擎，Spark Streaming是基于微小批次的伪实时处理引擎，在实时处理上Flink更有优势。

* **总结**

从底层来看Flink是真正的面向实时的处理引擎，Spark Streaming是基于微小批次的伪实时处理引擎，在实时处理上Flink更有优势。



* **延迟**

