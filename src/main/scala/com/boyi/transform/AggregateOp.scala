package com.boyi.transform

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object AggregateOp {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data =  env.fromCollection(List(("java" , 1) , ("java", 1) ,("scala" , 1)))
    // 3. 使用`groupBy`按照单词进行分组
    val groupedDataStream = data.groupBy(0)

    // 4. 使用`aggregate`对每个分组进行`SUM`统计
    val resultDataStream = groupedDataStream.aggregate(Aggregations.SUM, 1)

    // 4.打印输出
    resultDataStream.print()

  }
}
