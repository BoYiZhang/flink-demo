package com.boyi.datasource



import java.nio.charset.Charset

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FileSource {

  def main(args : Array[String]) : Unit = {
    // 1. 获取流处理运行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 读取文件
    val data : DataStream[String] = env.readTextFile("hdfs://h23:8020/tmp/test/score.csv")
    // 3. 打印数据
    data.print()
    // 4. 执行程序
    env.execute()

  }

}
