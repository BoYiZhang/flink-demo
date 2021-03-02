package com.boyi.datasource


import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object CollectionSource {


  def main(args: Array[String]): Unit ={
    // 1. 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度,默认和CPU的核数相同
    env.setParallelism(1)


    //0.用element创建DataStream(fromElements)
    val ds0 : DataStream[Int] = env.fromElements(1,3234,55,65,74523,1)
//    ds0.print()


    //1.用Tuple创建DataStream(fromElements)
    val ds1: DataStream[(Int,String)] = env.fromElements((1,"bo"),(2,"yi"))
//    ds1.print()

    //2.用Array创建DataStream
    val ds2: DataStream[String] = env.fromCollection(Array("bo","yi"))
//    ds2.print()

    //3.用ArrayBuffer创建DataStream
    val ds3 :DataStream[String] = env.fromCollection(ArrayBuffer("bo","yi"))
//    ds3.print()

    //4.用List创建DataStream
    val ds4 : DataStream[String] = env.fromCollection(List("bo","yi"))
//    ds4.print()

    //5.用ListBuffer创建DataStream
    val ds5 : DataStream[String] =  env.fromCollection(ListBuffer("BO","YI"))
//    ds5.print()
    //6.用Vector创建DataStream
    val ds6 : DataStream[String] = env.fromCollection(Vector("bo","yi","!!!"))
//    ds6.print()

    //7.用Queue创建DataStream
    val ds7: DataStream[String] = env.fromCollection(mutable.Queue("bo", "yi","flink","!!!"))
//    ds7.print()

    //8.用Stack创建DataStream
    val ds8: DataStream[String] = env.fromCollection(mutable.Stack("bo", "yi","flink","!!!"))
//    ds8.print()


    //9.用Stream创建DataStream
    val ds9: DataStream[String] = env.fromCollection(Stream("bo", "yi","flink","!!!"))
//    ds9.print()


    //10.用Seq创建DataStream
    val ds10: DataStream[String] = env.fromCollection(Seq("bo", "yi","flink","!!!"))
//    ds10.print()


    //11.用Set创建DataStream(不支持)
    //val ds11: DataStream[String] = env.fromCollection(Set("bo", "yi","flink","!!!"))
    //ds11.print()


    //12.用Iterable创建DataStream(不支持)
    //val ds12: DataStream[String] = env.fromCollection(Iterable("bo", "yi","flink","!!!"))
    //ds12.print()


    //13.用ArraySeq创建DataStream
    val ds13: DataStream[String] = env.fromCollection(mutable.ArraySeq("bo", "yi","flink","!!!"))
//    ds13.print()


//    //14.用ArrayStack创建DataStream
//    val ds14: DataStream[String] = env.fromCollection(mutable.ArrayStack("bo", "yi","flink","!!!"))
//    ds14.print()


    //15.用Map创建DataStream(不支持)
    //val ds15: DataStream[(Int, String)] = env.fromCollection(Map(1 -> "spark", 2 ->"flink"))
    //ds15.print()


//    //16.用Range创建DataStream
    val ds16: DataStream[Int] = env.fromCollection(Range(1, 9))
//    ds16.print()


//    //17. Sequence创建DataStream
    val ds17: DataStream[Long] = env.fromSequence(1, 9)
    ds17.print()

    // 执行任务,但是在流环境下,必须手动执行任务
    env.execute()



  }




}
