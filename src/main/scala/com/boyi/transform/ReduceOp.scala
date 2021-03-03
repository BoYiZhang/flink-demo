package com.boyi.transform

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

object ReduceOp {

  def main(args : Array[String]):Unit ={
    // 1. 创建流处理环境
    val env : ExecutionEnvironment  = ExecutionEnvironment.getExecutionEnvironment

    // 2.用fromCollection创建DataStream(fromCollection)
    val data : DataSet[(String,Int)]  = env.fromCollection(List(("java" , 1) , ("java", 1) ,("java" , 1) ))
    // 3.处理数据
    val res : DataSet[(String,Int)]  = data.reduce((x1,x2)=>{
      //注意, 这里只是无脑聚合, 最终的输出结果为: (flink,4)
      (x1._1,x1._2+x2._2)
    })
    // 4.打印输出
    res.print()
  }

}
