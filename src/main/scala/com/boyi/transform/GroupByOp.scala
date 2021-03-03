package com.boyi.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object GroupByOp {
  def main(args : Array[String]):Unit ={
    // 1. 创建流处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data = env.fromCollection(List(("java" , 1) , ("java", 1) ,("scala" , 1)  ))
    // 3. 使用`groupBy`按照单词进行分组
    val groupData = data.groupBy(_._1)
    // 4. 使用`reduce`对每个分组进行统计
    val reduceData = groupData.reduce((x1,x2) =>{
      (x1._1, x1._2+x2._2)
    })
    // 5.执行任务/输出结果
    reduceData.print()
  }

}
