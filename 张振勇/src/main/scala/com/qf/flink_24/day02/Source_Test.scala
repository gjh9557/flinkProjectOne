package com.qf.flink_24.day02

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
/**
  * Description：
  * Copyright (c) ，2019 ， zzy 
  * This program is protected by copyright laws. 
  * Date： 2019年12月18日 
  *
  * @author 张振勇
  * @version : 0.1
  */
object Source_Test {
  def main(args: Array[String]): Unit = {
   //env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1.从自定义集合中读取source
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("senor_1", 1547718199, 35.1232243242),
      SensorReading("senor_2", 1547718299, 15.1232243242),
      SensorReading("senor_3", 1547718399, 25.1232243242),
      SensorReading("senor_4", 1547719199, 5.1232243242)
    ))
//    stream1.print("stream1").setParallelism(1)
    //2.从文件中读取数据
    val stream2 = env.readTextFile("D:\\ideaWork\\Flink_24\\src\\main\\resources\\senor.txt")
//    stream2.print("stream2").setParallelism(1)
    //3.从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","mini:9092")
    properties.setProperty("group.id","gp24")
    properties.setProperty("auto.offset.reset","latest")

    val stream3 = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(),properties))
    stream3.print("stream3").setParallelism(1)

    env.execute("source test")
  }

}
//温度传感器读取样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )
