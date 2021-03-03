package com.boyi.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
object DistinctOp {

  def main(args : Array[String]) : Unit = {
    // 1. 创建流处理环境
    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data =  env.fromCollection(List(("java" , 1) , ("java", 2) ,("scala" , 1)))

    // 3. 使用`distinct`指定按照哪个字段来进行去重
    val res = data.distinct(0)

    // 4.打印输出
    res.print()


  }

}
