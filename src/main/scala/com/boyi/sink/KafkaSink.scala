package com.boyi.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object KafkaSink {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data = env.fromCollection(List("flink","Spark"))
    // 3.构造Kafka Sink
    val pro: Properties = new Properties
    pro.setProperty("bootstrap.servers", " 192.168.101.30:9092")
    val kafkaSink = new FlinkKafkaProducer[String]("test",new SimpleStringSchema(),pro)
    // 4.打印输出
    data.addSink(kafkaSink)

    // 5.执行任务
    env.execute()

  }
}
