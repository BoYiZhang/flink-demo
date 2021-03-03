package com.boyi.transform

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


//  使用map操作，将以下数据
//
//  "1,张三", "2,李四", "3,王五", "4,赵六"
//  转换为一个scala的样例类。
//
//  步骤
//    获取ExecutionEnvironment运行环境
//    使用fromCollection构建数据源
//    创建一个User样例类
//    使用map操作执行转换
//    打印测试

object MapOp {

  // User实体类
  case class User( id:String , name :String )

  def main(args : Array[String]) : Unit = {
    // 1. 创建流处理环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data : DataStream[String]  = env.fromCollection(Array("1,张三", "2,李四", "3,王五", "4,赵六"))

    // 3.处理数据
    data.map( text => {
      val uStr:Array[String] = text.split(",")
      User(uStr(0),uStr(1))
    })

    // 4.打印输出
    data.print()

    // 5.执行任务
    env.execute()


  }

}
