package com.boyi.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object UnionOp {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data1 = env.fromCollection(List("hadoop", "hive", "flume"))

    val data2 = env.fromCollection(List("hadoop", "hive", "spark"))

    // 3.处理数据
    val res = data1.union(data2)
    // 4.打印输出
    res.print()

  }
}
