package com.boyi.transform

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FilterOp {

  def main(args : Array[String]):Unit ={
    // 1. 创建流处理环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

    // 2.用fromCollection创建DataStream(fromCollection)
    val data : DataStream[String] = env.fromCollection(List("hadoop", "hive", "spark", "flink"))
    // 3.处理数据
    val res = data.filter(x => {x.startsWith("h")})
    // 4.打印输出
    res.print()

    // 5.执行任务
    env.execute()
  }

}
