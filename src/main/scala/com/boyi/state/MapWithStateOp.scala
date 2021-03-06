package com.boyi.state

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._


/**
 *
 *
 * mapWithState 函数说明 :
 *
 * mapWithState[R, S]
 *  (fun : scala.Function2[T, scala.Option[S], scala.Tuple2[ R, scala.Option[S] ]])
 * R: TypeInformation （return返回类型）
 * S: TypeInformation （stateful状态类型）
 * T （input 输入类型）
 * fun: (T, Option[S]) => (R, Option[S]) 函数将输入泛型转化了R，状态泛型没有变化
 *
 *
 *
 * 案例 : 计算 动态计算 每个key的平均数
 */
object MapWithStateOp extends  App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.fromCollection(List(
    (1L, 3L),
    (1L, 5L),
    (1L, 7L),
    (1L, 4L),
    (1L, 2L),
    (2L,10L),
    (2L,2L)
  )).keyBy(_._1)
    .mapWithState[(Long,Long),(Long,Long)]((input: (Long,Long), out: Option[(Long,Long)]) =>
      out match {
        case Some(state) => {
          // return  : (key,平均数) , ()
          // state   : (value个数,value总和)
          ((input._1,((state._2+input._2)/(state._1+1))),Some((state._1+1, state._2+input._2)))
        }
        case None => ((input._1,input._2),Some((1L,input._2)))
      }).map(x => {
    "key : "+x._1+" ===> avg : "+x._2
  }).print()


  env.execute("MapWithStateOp")
}
