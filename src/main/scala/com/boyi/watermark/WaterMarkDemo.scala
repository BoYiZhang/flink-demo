package com.boyi.watermark

import java.util.concurrent.TimeUnit
import java.util.{Date, UUID}

import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows




// assignTimestampsAndWatermarks
object WaterMarkDemo {
  // 创建一个订单样例类`Order`，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  case class Order (orderId: String, userId: Int, money: Long, timestamp: Long)

  def main(args : Array[String]) : Unit = {
    // 1. 创建流处理运行环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //   2. 设置处理时间为`EventTime`
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 4. 创建一个自定义数据源
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


    // 5. 添加Watermark


    val watermarkDataStream = orderDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Order] {
      var currentTimestamp = 0L
      val delayTime = 2000

      override def getCurrentWatermark: Watermark = {
        //   - 允许延迟2秒
        // - 在获取Watermark方法中，打印Watermark时间、当前事件时间和当前系统时间
        val watermark = new Watermark(currentTimestamp - delayTime)
        val dateFormat = FastDateFormat.getInstance("HH:mm:ss")

        println(s"当前Watermark时间:${dateFormat.format(watermark.getTimestamp)}, 当前事件时间: ${dateFormat.format(currentTimestamp)}, 当前系统时间: ${dateFormat.format(System.currentTimeMillis())}")
        watermark
      }

      override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
        val timestamp = element.timestamp
        currentTimestamp = Math.max(currentTimestamp, timestamp)
        currentTimestamp
      }
    })

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
