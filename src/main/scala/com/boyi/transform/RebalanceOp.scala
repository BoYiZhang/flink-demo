package com.boyi.transform


import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}


object RebalanceOp {
  def main(args: Array[String]): Unit = {
    // 1. 获取`ExecutionEnvironment`运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 2. 使用`env.generateSequence`创建0-100的并行数据
    val numDataStream = env.generateSequence(0, 100)

    // 3. 使用`fiter`过滤出来`大于8`的数字
    val filterDataStream = numDataStream.filter(_ > 8)

    // 4. 是否设置rebalance
    // filterDataStream.rebalance()

    // 5. 使用map操作传入`RichMapFunction`，将当前子任务的ID和数字构建成一个元组
    val resultDataStream = filterDataStream.map(new RichMapFunction[Long, (Long, Long)] {
      override def map(in: Long): (Long, Long) = {
        (getRuntimeContext.getIndexOfThisSubtask, in)
      }
    })

    // 6. 打印测试
    resultDataStream.print()
  }
}
