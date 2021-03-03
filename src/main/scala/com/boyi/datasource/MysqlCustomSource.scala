package com.boyi.datasource

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object MysqlCustomSource {
  def main(args : Array[String]) : Unit = {

    // 1. env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2 使用自定义Source
    val mySqlDataStream: DataStream[(Int, String, String, String)] = env.addSource(new MysqlSource)

    // 3. 打印结果
    mySqlDataStream.print()

    // 4. 执行任务
    env.execute()

  }

  //  1. 自定义Source,继承自RichSourceFunction
  class MysqlSource extends RichSourceFunction[(Int,String,String,String)]{

    //  2. 实现run方法
    override def run(sourceContext: SourceFunction.SourceContext[(Int, String, String, String)]): Unit = {
      //    1. 加载驱动
      Class.forName("com.mysql.jdbc.Driver")
      //    2. 创建连接
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/tmp?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "root", "root")
      //    3. 创建PreparedStatement
      val sql = "select id,username,password,name from user"
      val ps: PreparedStatement = connection.prepareStatement(sql)
      //    4. 执行查询
      val resultSet: ResultSet = ps.executeQuery()
      //    5. 遍历查询结果,收集数据
      while(resultSet.next()){
        val id = resultSet.getInt("id")
        val username = resultSet.getString("username")
        val password = resultSet.getString("password")
        val name = resultSet.getString("name")

        // 收集数据
        sourceContext.collect((id,username,password,name))
      }
    }

    override def cancel(): Unit = ()
  }



}
