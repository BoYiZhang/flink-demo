package com.boyi.watermark

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.{Date, UUID}

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

// 在非数据源的操作之后使用 WaterMark

object WaterMarkDemo {
  // 创建一个订单样例类`Order`，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  case class Order (orderId: String, userId: Int, money: Long, timestamp: Long)

  def main(args : Array[String]) : Unit = {
    // 1. 创建流处理运行环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2. 创建一个自定义数据源
    val orderDataStream = env.addSource(new RichSourceFunction[Order] {
      var isRunning = true
      override def run(sourceContext: SourceFunction.SourceContext[Order]): Unit = {
        while (isRunning){
          //   - 随机生成订单ID（UUID）
          // - 随机生成用户ID（0-2）
          // - 随机生成订单金额（0-100）
          // - 时间戳为当前系统时间
          // - 每隔1秒生成一个订单
          val order = Order(UUID.randomUUID().toString,Random.nextInt(3),Random.nextInt(101),new Date().getTime)

          sourceContext.collect(order)
          TimeUnit.SECONDS.sleep(2)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })


    // 3. 添加Watermark
    val watermarkDataStream =  orderDataStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[Order](Duration.ofSeconds(20))
      .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
          element.timestamp
        }
      })
    )


    // 6. 按照用户进行分流
    // 7. 设置5秒的时间窗口
    // 8. 进行聚合计算
    // 9. 打印结果数据
    // 10. 启动执行流处理
    watermarkDataStream.keyBy(_.userId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce {
        (order1, order2) =>
          Order(order2.orderId, order2.userId, order1.money + order2.money, 0)
      }
      .print()
    env.execute("WarkMarkDemoJob")



  }


}
