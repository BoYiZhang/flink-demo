package com.boyi.broadcast

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * 分布式缓存
 */
object BatchDemoDisCache {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    //1:注册文件
    env.registerCachedFile("/opt/a/tmp/BatchDemoDisCache.txt","BatchDemoDisCache.txt")

    //读取数据
    val data = env.fromElements("a","b","c","d")
    val result = data.map(new RichMapFunction[String,String] {

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //访问数据
        val myFile = getRuntimeContext.getDistributedCache.getFile("BatchDemoDisCache.txt")
        val lines = FileUtils.readLines(myFile)
        val it = lines.iterator()
        while (it.hasNext){
          val line = it.next();
          println("line:"+line)
        }
      }
      override def map(value: String) = {
        value
      }
    })
    result.print()
  }
}
