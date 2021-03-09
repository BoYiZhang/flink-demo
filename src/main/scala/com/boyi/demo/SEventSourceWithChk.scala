//package com.boyi.demo
//
//import java.time.Duration
//import java.util.Date
//import java.util.concurrent.TimeUnit
//
//import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
//import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
//import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
//import org.apache.flink.api.java.tuple.Tuple
//import org.apache.flink.api.scala.createTypeInformation
//import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
//import org.apache.flink.runtime.state.filesystem.FsStateBackend
//import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
//import org.apache.flink.streaming.api.scala.function.WindowFunction
//import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//import sun.util.resources.cldr.chr.TimeZoneNames_chr
//
//import scala.collection.mutable.ListBuffer
//
///**
//
//####  需求
//
//假定用户需要每隔`1秒`钟需要统计`4秒`中窗口中`数据的量`，然后对统计的结果值进行`checkpoint`处理。
//
//窗口的长度: 4s
//
//窗口的滑动时间: 1s
//
//求数据量的总数
//
//checkpoint的支持
//
//#### 数据规划
//    使用自定义算子每秒钟产生大约10000条数据。
//
//    产生的数据为一个四元组(Long，String，String，Integer)—------(id,name,info,count)
//
//    数据经统计后，统计结果打印到终端输出
//
//    打印输出的结果为Long类型的数据
//
//#### 开发思路
//
//    source算子每隔1秒钟发送10000条数据，并注入到Window算子中。
//
//    window算子每隔1秒钟统计一次最近4秒钟内数据数量。
//
//    每隔1秒钟将统计结果打印到终端
//
//    每隔6秒钟触发一次checkpoint，然后将checkpoint的结果保存到HDFS或本地文件中。
//
//
//
//#### 开发步骤
//
// **开发自定义数据源**
//
// 	1. 自定义样例类(id: Long, name: String, info: String, count: Int)
// 	2. 自定义数据源,继承RichSourceFunction
// 	3. 实现run方法, 每秒钟向流中注入10000个样例类
//
//
//
// **开发自定义状态**
//
// 	1. 继承Serializable
// 	2. 为总数count提供set和get方法
//
//
// **开发自定义Window和检查点**
//
// 	1. 继承WindowFunction
// 	2. 重写apply方法,对窗口数据进行总数累加
// 	3. 继承ListCheckpointed
// 	4. 重写snapshotState,制作自定义快照
// 	5. 重写restoreState,恢复自定义快照
//
// **开发主业务**
//
//1. 流处理环境
//
//2. 开启checkpoint,间隔时间为6s
//
//3. 设置checkpoint位置
//
//4. 设置处理时间为事件时间
//
//5. 添加数据源
//
//6. 添加水印支持
//
//7. keyby分组
//
//8. 设置滑动窗口,窗口时间为4s
//
//9. 指定自定义窗口
//
//10. 打印结果
//
//11. 执行任务
//
// */
//object SEventSourceWithChk {
//
//
//
//  /**
//   * 该段代码是流图定义代码，具体实现业务流程，另外，代码中窗口的触发时间使 用了event time。
//   */
//  def main(args:Array[String]) ={
//    // 1. 流处理环境
//    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    // 2. 开启checkpoint,间隔时间为6s 设置checkpoint模式, EXACTLY_ONCE为默认值,这句可以省略
//    env.enableCheckpointing(6000,CheckpointingMode.EXACTLY_ONCE)
//    // 3. 设置checkpoint位置
//    //    env.setStateBackend(new FsStateBackend("hdfs://h23:8020/flink-checkpoint/checkpoint/"))
//    env.setStateBackend(new FsStateBackend("file:///opt/a/tmp/flink_checkpoint"))
//
//    // 4.设置处理时间为 事件时间 [1.12版本已过时]
//    // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    // 5. 添加数据源
//    val source : DataStream[SEvent] = env.addSource(new SEventSourceWithChk)
//    // 6. 添加水印支持
//    val watermarkDataStream: DataStream[SEvent] = source.assignTimestampsAndWatermarks(
//      WatermarkStrategy.forBoundedOutOfOrderness[SEvent](Duration.ofSeconds(10))
//        .withTimestampAssigner(new SerializableTimestampAssigner[SEvent]{
//          override def extractTimestamp(element: SEvent, l: Long): Long = {
//            element.createTime
//          }
//        })
//    )
//    // 7. keyby分组
//    val keyStream: KeyedStream[SEvent,Long] =watermarkDataStream.keyBy(_.id)
//
//    // 8. 设置滑动窗口,窗口时间为4s
//    val windowedStream: WindowedStream[SEvent,Long,Nothing] = keyStream.window(TumblingProcessingTimeWindows.of(Time.seconds(4),Time.seconds(1)))
//
//    // 9. 指定自定义窗口
//    val result: DataStream[Long] =  windowedStream.apply(new WindowStatisticWithChk)
//    // 10. 打印结果
//    result.print()
//    // 11. 执行任务
//    env.execute()
//
//
//  }
//
//
//
//
//}
//
//
////样例类-->发送数据形式
//case class SEvent(id: Long, name: String, info: String, count: Int,createTime:Long)
//
///**
// * 自定义数据源,继承RichSourceFunction
// * 实现run方法和cancel方法
// */
//
//class SEventSourceWithChk extends RichSourceFunction{
//  private var isRunning = true
//
//  // source算子的逻辑，即:每秒钟向流图中注入10000个元组
//  override def run(ctx: SourceFunction.SourceContext[SEvent]): Unit = {
//    while (isRunning) {
//      for (i <- 0 until 10000) {
//        ctx.collect(SEvent(1, "hello-" + i, "test-info", 1,new Date().getTime()))
//      }
//      TimeUnit.SECONDS.sleep(1)
//    }
//  }
//
//  // 任务取消时调用
//  override def cancel(): Unit = {
//    isRunning = false
//  }
//}
//
///**
// * 该数据在算子制作快照时用于保存到目前为止算子记录的数据条数。
// * 用户自定义状态
// */
//class UDFState extends Serializable{
//  private  var count = 0L
//  // 设置用户自定义状态
//  def setState(s:Long) =  count = s
//  // 获取用户自定状态
//  def getState = count
//
//}
//
//
///**
// * 该段代码是window算子的代码，每当触发计算时统计窗口中元组数量。
// * 自定义Window,继承WindowFunction
// * WindowFunction[IN, OUT, KEY, W <: Window]
// * ListCheckpointed[UDFState]
// *
// */
//
//class WindowStatisticWithChk extends WindowFunction[SEvent, Long, Tuple,TimeWindow] with CheckpointedFunction[UDFState] {
//  private var total = 0L
//  private var state: ListState[SEvent] = _
//  /**
//   * window算子的实现逻辑，即:统计window中元组的数量
//   *
//   * @param key    keyby的类型
//   * @param window 窗口
//   * @param input  输入类型
//   * @param out    输出类型
//   */
//  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SEvent], out: Collector[Long]): Unit = {
//    var count = 0L
//    for (event <- input) {
//      count += 1L
//    }
//    total += count
//    out.collect(count)
//  }
//
//  override def snapshotState(context: FunctionSnapshotContext): Unit = {
//    val udfList: java.util.ArrayList[UDFState] = new java.util.ArrayList[UDFState]
//    val udfState = new UDFState
//    udfState.setState(total)
//    udfList.add(udfState)
//    udfList
//  }
//
//  override def initializeState(context: FunctionInitializationContext): Unit = {
//    state = context.getOperatorStateStore.getListState(
//      new ListStateDescriptor[SEvent](
//        "sourceState", createTypeInformation[SEvent]))
//  }
//
//
//}