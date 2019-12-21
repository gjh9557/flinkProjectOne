package com.qf.flink_24.day01

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * Description：
  * 错误记录:导错包import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
  *   导致data.flatMap(_.split("\\s"))一直报错
  * Copyright (c) ，2019 ， zzy 
  * This program is protected by copyright laws. 
  * Date： 2019年12月16日 
  *
  * @author 张振勇
  * @version : 0.1
  */
object WordCount_Scala {
  def main(args: Array[String]): Unit = {
    //1.获取netcat的port
    val port:Int = try{
      ParameterTool.fromArgs(args).getInt("port")
    }catch{
      case e:Exception => println("使用默认端口6666")
        6666
    }
    //2.flink 的上下文对象
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //3.获取数据
    val data = env.socketTextStream("mini",port)
    //4.引入flink的scala api
    import org.apache.flink.api.scala._
    //5.切分数据
    val splited = data.flatMap(_.split("\\s"))
    //6.过滤空值
    val filted = splited.filter(_.nonEmpty)
    //6.map改造数据格式
    val tup = filted.map((_,1))
    //7.分组
    val grouped = tup.keyBy(0)
    //8.实时统计
    val res = grouped.sum(1)
    //9.打印 (sink)
    res.print()
    //10.启动
    env.execute()

  }
}
