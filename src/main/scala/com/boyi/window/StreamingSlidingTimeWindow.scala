package com.boyi.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


// 要先打开 服务端,否则报错 ::     nc -lk 9999
// 滑动时间计数器窗口
object StreamingSlidingTimeWindow {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2.定义数据流来源
    val textStream = env.socketTextStream("localhost",9999)
    // 3.处理数据
    val data = textStream.flatMap(x=> x.split(" ")).filter( x=>{ null!=x && x.trim.length != 0}).map(x=>{(x,1)})

    //4. 根据key做聚合操作..
    val keyByData = data.keyBy(_._1)

    //5. 执行统计操作，步长为2, 每5秒钟统计一下各单词的数量
    // TumblingProcessingTimeWindows
    val result= keyByData.window(TumblingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(2))).sum(1)
    //6. 显示统计结果
    result.print()
    //7. 触发流计算
    env.execute()
  }
}
