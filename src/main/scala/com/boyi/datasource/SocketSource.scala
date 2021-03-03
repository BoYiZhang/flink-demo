package com.boyi.datasource

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


//  示例
//    编写Flink程序，接收 socket 的单词数据，并以空格进行单词拆分打印。
//    步骤
//      1. 获取流处理运行环境
//      2. 构建socket流数据源，并指定IP地址和端口号
//      3. 对接收到的数据进行空格拆分
//      4. 打印输出
//      5. 启动执行
//      6. 在Linux中，使用 nc -lk 端口号 监听端口，并发送单词
//
//  安装nc: yum install -y nc
//  nc -lk 6666 监听6666端口的信息


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
