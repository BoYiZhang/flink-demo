package com.boyi.transform

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
  //
  //  分别将以下数据，转换成国家、省份、城市三个维度的数据。
  //
  //  将以下数据
  //
  //    张三,中国,江西省,南昌市
  //    李四,中国,河北省,石家庄市
  //    Tom,America,NewYork,Manhattan
  //  转换为
  //
  //    (张三,中国)
  //    (张三,中国,江西省)
  //    (张三,中国,江西省,江西省)
  //    (李四,中国)
  //    (李四,中国,河北省)
  //    (李四,中国,河北省,河北省)
  //    (Tom,America)
  //    (Tom,America,NewYork)
  //    (Tom,America,NewYork,NewYork)

object FlatMapOp {



  def main(args : Array[String]) : Unit = {
    // 1. 创建流处理环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data : DataStream[String] =  env.fromCollection(List("张三,中国,江西省,南昌市","李四,中国,河北省,石家庄市","Tom,America,NewYork,Manhattan"))
    // 3.处理数据
    val res = data.flatMap(text => {
      val dataStr : Array[String] = text.split(",")

      List(
        dataStr(0)+","+dataStr(1),
        dataStr(0)+","+dataStr(1)+","+dataStr(2),
        dataStr(0)+","+dataStr(1)+","+dataStr(2)+","+dataStr(3),
      )

    })
    // 4.打印输出
    res.print()

    // 5.执行任务
    env.execute()

  }

}
