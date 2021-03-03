package com.boyi.sink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SinkCollection {

  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data = env.fromCollection(List((19, "zhangsan", 178.8),  (17, "lisi", 168.8), (18, "wangwu", 184.8),  (21, "zhaoliu", 164.8)  ))
    // 3.处理数据

    // 4.打印输出
    data.print()

    data.printToErr()

    // data 的数据为批处理的时候可以使用collect
    // print(data.collect())

    // 5.执行任务
    env.execute()

  }

}
