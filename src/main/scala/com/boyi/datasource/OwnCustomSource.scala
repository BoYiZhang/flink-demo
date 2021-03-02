package com.boyi.datasource

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}


import scala.util.Random


//  自定义数据源, 每1秒钟随机生成一条订单信息( 订单ID 、 用户ID 、 订单金额 、 时间戳 )
//  要求:
//    随机生成订单ID（UUID）
//    随机生成用户ID（0-2）
//    随机生成订单金额（0-100）
//    时间戳为当前系统时间

//    开发步骤:
//      1. 创建订单样例类
//      2. 获取流处理环境
//      3. 创建自定义数据源
//          循环1000次
//          随机构建订单信息
//          上下文收集数据
//          每隔一秒执行一次循环
//      4. 打印数据
//      5. 执行任务


object OwnCustomSource {


  // 创建一个订单样例类Order，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  case class Order(id : String , userId : Int , money : Long , createTime : Long)



  def main(args : Array[String]) : Unit = {
    // 1. 获取流处理运行环境r
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 创建一个自定义数据源
    val ownCustomSource : DataStream[Order] =   env.addSource(new RichSourceFunction[Order] {
      override def run(sourceContext: SourceFunction.SourceContext[Order]): Unit = {

        for (i <- 0 until 1000){
          // 随机生成订单ID（UUID）
          val id = UUID.randomUUID().toString
          // 随机生成用户ID（0-2）
          val userId = Random.nextInt(3)
          // 随机生成订单金额（0-100）
          val money =  Random.nextInt(101)
          // 时间戳为当前系统时间
          val createTime = System.currentTimeMillis()
          // 收集数据
          sourceContext.collect(Order(id,userId,money, createTime))
          // 每隔1秒生成一个订单
          TimeUnit.SECONDS.sleep(1)
        }

      }

      override def cancel(): Unit = ()
    })

    ownCustomSource.print()

    env.execute()





  }

}
