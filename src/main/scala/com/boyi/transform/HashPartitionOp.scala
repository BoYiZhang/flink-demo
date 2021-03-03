package com.boyi.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object HashPartitionOp {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 设置并行度为`2`
    env.setParallelism(2)

    // 2.用fromCollection创建DataStream(fromCollection)
    val data = env.fromCollection(List(1,1,1,1,1,1,1,2,2,2,2,2))
    // 3.处理数据
    val partionData = data.partitionByHash(_.toString)

    // 4. 调用`writeAsText`写入文件到`/opt/a/tmp/testPartion`目录中
    partionData.writeAsText("/opt/a/tmp/testPartion")


    // 5.打印输出
    partionData.print()




  }
}
