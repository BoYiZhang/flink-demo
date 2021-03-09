package com.boyi.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector


/**
 *
 * 实现了一个简单的计数窗口。 我们把元组的第一个元素当作 key（在示例中都 key 都是 “1”）。
 * 该函数将出现的次数以及总和存储在 “ValueState” 中。
 * 一旦出现次数达到 2，则将平均值发送到下游，并清除状态重新开始。
 *
 * 请注意，我们会为每个不同的 key（元组中第一个元素）保存一个单独的值。
 *
 */
class CountWindowTTLAverage extends RichFlatMapFunction[(Long,Long),(Long,Long)] {

  private var sum : ValueState[(Long,Long)] = _

  val ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build()


  override def flatMap(input: (Long, Long), out : Collector[(Long, Long)]): Unit = {
    // 获取当前的状态值
    val tmpCurrentSum = sum.value()

    val currentSum = if(tmpCurrentSum != null) {
      tmpCurrentSum
    }else{
      (0L,0L)
    }

    // 获取当前值
    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    // 更新state
    sum.update(newSum)


    // 如果计算达到2, 计算平均值,并清除state
    if (newSum._1 >= 2) {
      out.collect((input._1,newSum._2/newSum._1))
      sum.clear()
    }



  }

  override def open(parameters: Configuration): Unit = {
    val stateDescriptor = new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    stateDescriptor.enableTimeToLive(ttlConfig)

    sum = getRuntimeContext.getState(stateDescriptor)
  }

}


object ExampleCountTTLWindowAverage extends  App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.fromCollection(List(
    (1L, 3L),
    (1L, 5L),
    (1L, 7L),
    (1L, 4L),
    (1L, 2L)
  )).keyBy(_._1)
    .flatMap(new CountWindowTTLAverage())
    .print()

  // the printed output will be (1,4) and (1,5)

  env.execute("ExampleKeyedState")
}
