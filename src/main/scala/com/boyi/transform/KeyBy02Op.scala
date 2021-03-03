package com.boyi.transform

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

// 读取socket数据源, 进行单词的计数
object KeyBy02Op {


  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(2)
    // 2.用fromCollection创建DataStream(fromCollection)
    val data : DataStream[String] = env.readTextFile("hdfs://h23:8020/tmp/test/score.csv")
    // 3.处理数据
    val res = data.flatMap(x => x.split(",")).map(x=>(x,1))

    // 3.打印输出
    res.print()
    // 4.执行任务
    env.execute()
  }

}
