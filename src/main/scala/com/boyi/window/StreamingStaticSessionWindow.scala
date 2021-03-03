package com.boyi.window


import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time


object StreamingStaticSessionWindow {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.定义数据流来源
    val scoketText : DataStream[String] = env.socketTextStream("localhost",9999)
    // 3.处理数据
    val data = scoketText.flatMap(_.split(" ")).filter( x=>{x != null && x.trim.length >0 }).map(x=>(x,1))
    //4. 根据key做聚合操作..
    val keyedStream : KeyedStream[(String,Int),String] = data.keyBy(_._1)
    //5. session 超时设置 5秒
    val res = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5))).sum(1)
    //6. 显示统计结果
    res.print()
    //7. 触发流计算
    env.execute()

  }
}
