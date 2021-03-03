package com.boyi.transform

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object SortPartionOp {



  def main(args : Array[String]) : Unit = {
    // 1. 创建流处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data = env.fromCollection(List("hadoop", "hadoop", "hadoop", "hive", "hive", "spark", "spark", "flink"))
    // 3.处理数据
    val res = data.sortPartition(_.toString,Order.DESCENDING)
    // 4.打印输出
    res.print()
  }

}
