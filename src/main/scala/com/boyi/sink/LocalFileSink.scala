package com.boyi.sink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object LocalFileSink {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 2.用fromCollection创建DataStream(fromCollection)
    val data = env.fromCollection(List((1,"flink"),(2,"sink")))
    // 3.处理数据

    // 4.打印输出
    data.writeAsText("/opt/a/tmp/FileSink.txt",WriteMode.OVERWRITE)

    // 5.执行任务
    env.execute()

  }
}
