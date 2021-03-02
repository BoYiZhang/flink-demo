package com.boyi.datasource

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.CommonClientConfigs

object KafkaCustomSource {

  def main(args : Array[String])  :  Unit = {
    // 1. 创建流式环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2 .指定kafak相关信息
    val kafkaCluster = "k01:9092,k02:9092"
    val kafkaTopic = "test"
    // 3. 创建Kafka数据流
    val props = new Properties()
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,kafkaCluster)

    val flinkKafkaConsumer = new FlinkKafkaConsumer[String](kafkaTopic,new SimpleStringSchema(),props)

    //4 .设置数据源

    val data : DataStream[String] = env.addSource(flinkKafkaConsumer)

    // 5. 打印数据
    data.print()

    // 6.执行任务
    env.execute()


  }


}
