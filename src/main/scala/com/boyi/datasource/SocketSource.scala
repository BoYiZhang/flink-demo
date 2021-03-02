package com.boyi.datasource

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SocketSource {

  def main (args : Array[String]) : Unit = {
    //1. 获取流处理运行环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

    // 2. 构建socket流数据源，并指定IP地址和端口号
    val data : DataStream[String] = env.socketTextStream("localhost",6666)

    // 3. 转换,以空格拆分单词
    val res = data.flatMap(_.split(" "))
    // 4. 打印输出
    res.print()

    // 5. 启动执行
    env.execute("WordCount_Stream")

  }


}
