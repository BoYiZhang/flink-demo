package com.boyi.transform

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

object MapPartitionOp {

  case class User(id:String,name:String)

  def main(args : Array[String]) : Unit ={
    // 1. 创建流处理环境
    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data: DataSet[String] = env.fromCollection(List("1,张三", "2,李四", "3,王五", "4,赵六"))
    // 3.处理数据
    val res : DataSet[User]  =  data.mapPartition(x=>{
      x.map(x =>{
        val fieldArr = x.split(",")
        User(fieldArr(0),fieldArr(1))
      })
    })
    // 4.打印输出
    res.print()

  }

}
