package com.boyi.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object JoinOp {
  // 学科Subject（学科ID、学科名字）
  case class Subject(id:Int, name:String)

  // 成绩Score（唯一ID、学生姓名、学科ID、分数）
  case class Score(id:Int, name:String, subjectId:Int, score:Double)


  def main(args: Array[String]): Unit = {
    // 1. 创建流处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val socreData = env.readCsvFile[Score]("hdfs://h23:8020/tmp/test/score.csv")

    val subjectData = env.readCsvFile[Subject]("hdfs://h23:8020/tmp/test/subject.csv")

    // 3.处理数据
    val joinData = socreData.join(subjectData).where(2).equalTo(0)

    // 4.打印输出

    joinData.print()

  }

}
