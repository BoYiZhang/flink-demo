package com.boyi.transform

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

object ReduceGroupOp {
  def main(args:Array[String]):Unit={

    // 1. 创建流处理环境
    val env : ExecutionEnvironment  = ExecutionEnvironment.getExecutionEnvironment
    // 2.用fromCollection创建DataStream(fromCollection)
    val data = env.fromCollection(List(("java" , 1) , ("java", 1) ,("scala" , 1)))
    // 3.使用`groupBy`按照单词进行分组
    val groupByData= data.groupBy(_._1)

    // 4.使用`reduceGroup`对每个分组进行统计
//    val reduceGroupData =  groupByData.reduceGroup { iter =>{
//      iter.reduce{(wc1, wc2) => (wc1._1,wc1._2 + wc2._2)}
//    } }

    val reduceGroupData =  groupByData.reduceGroup(x=>{
      x.reduce((x1,x2)=>{(x1._1,x1._2+x2._2)})
    })

    // 5.打印输出
    reduceGroupData.print()

  }
}
