package com.boyi.watermark

import java.time.Duration
import java.util.Properties

import org.apache.flink.api.common.eventtime. WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.CommonClientConfigs


//  直接在数据源上使用WaterMark



//当使用 Apache Kafka 连接器作为数据源时，每个 Kafka 分区可能有一个简单的事件时间模式（递增的时间戳或有界无序）。
// 然而，当使用 Kafka 数据源时，多个分区常常并行使用，
// 因此交错来自各个分区的事件数据就会破坏每个分区的事件时间模式（这是 Kafka 消费客户端所固有的）。
//
// 在这种情况下，你可以使用 Flink 中可识别 Kafka 分区的 watermark 生成机制。
// 使用此特性，将在 Kafka 消费端内部针对每个 Kafka 分区生成 watermark，
// 并且不同分区 watermark 的合并方式与在数据流 shuffle 时的合并方式相同。
//
//例如，
// 如果每个 Kafka 分区中的事件时间戳严格递增，则使用时间戳单调递增按分区生成的 watermark 将生成完美的全局 watermark。
//
// 注意，我们在示例中未使用 TimestampAssigner，而是使用了 Kafka 记录自身的时间戳。
//
object KafkaDataSourceWaterMarkDemo {

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
    kafkaSource.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(20)))

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
