package com.boyi.window



import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object StreamWindowApply {

  def main(args: Array[String]): Unit = {
    // 1. 获取流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 构建socket流数据源，并指定IP地址和端口号
    val socketDataStream = env.socketTextStream("localhost", 9999)

    // 3. 对接收到的数据转换成单词元组
    val wordcountDataStream: DataStream[(String, Int)] = socketDataStream.flatMap {
      text =>
        text.split(" ").map(_ -> 1)
    }

    // 4. 使用`keyBy`进行分流（分组）
    val groupedDataStream = wordcountDataStream.keyBy(_._1)

    // 5. 使用`TumblingProcessingTimeWindows`指定窗口的长度（每3秒计算一次）
    val windowedDataStream = groupedDataStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

    // 6. 实现一个WindowFunction匿名内部类
    val resultDataStream: DataStream[(String, Int)] = windowedDataStream.apply(new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {

      //   - 在apply方法中实现聚合计算
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val resultWordCount: (String, Int) = input.reduce {
          (wc1, wc2) =>
            (wc1._1, wc1._2 + wc2._2)
        }
        //   - 使用Collector.collect收集数据
        out.collect(resultWordCount)
      }
    })

    // 7. 打印输出
    resultDataStream.print()

    // 8. 启动执行
    env.execute("App")

  }
}

