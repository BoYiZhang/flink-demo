package com.boyi.watermark

import java.time.Duration
import java.util.Properties

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.CommonClientConfigs


/**
 *
 *
 * 如果数据源中的某一个分区/分片在一段时间内未发送事件数据，
 * 则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。
 *
 * 我们称这类数据源为空闲输入或空闲源。
 *
 * 在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。
 * 由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化。
 *
 * 为了解决这个问题，
 * 你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。
 *
 * WatermarkStrategy 为此提供了一个工具接口
 * WatermarkStrategy
 * .forBoundedOutOfOrderness[(Long, String)](Duration.ofSeconds(20))
 * .withIdleness(Duration.ofMinutes(1))
 *
 */
object FreeDataSourceWaterMark {

  def main(args : Array[String]) : Unit = {
    // 1. 创建流处理运行环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaCluster = "192.168.101.30:9092"
    val kafkaTopic = "test"
    // 2. 创建Kafka数据流
    val props = new Properties()
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,kafkaCluster)

    val kafkaSource = new FlinkKafkaConsumer(kafkaTopic,new SimpleStringSchema(),props)

    // 3. 设置watermark
    kafkaSource.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(20))
        .withIdleness(Duration.ofMinutes(1))
    )

    // 4. 添加数据源
    val watermarkDataStream  = env.addSource(kafkaSource)

    // 5. 处理数据
    watermarkDataStream.flatMap(_.split(" "))
      .map(x => (x,1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .reduce((x1,x2) =>(x1._1, x1._2+x1._2))
      .print()

    // 6. 执行
    env.execute("KafkaWaterMarkDemo")

  }
}
