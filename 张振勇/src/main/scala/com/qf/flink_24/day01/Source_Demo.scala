package com.qf.flink_24.day01

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

object Source_Demo {
  def main(args: Array[String]): Unit = {
    //创建一个上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    import org.apache.flink.api.scala._
      //基于文件、集合、socket,addSource
//    val ds1: DataStream[(String, Int)] = env.fromElements(("a",1),("b",2))
//    val ds2: DataStream[String] = env.readTextFile("D://test.txt")
//    val ds3: DataStream[String] = env.socketTextStream("node1",8989)
    //获取kafka信息
    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("zookeeper.connect","node242:2181")
    kafkaProperties.setProperty("boostrap.servers","node242:9092")
    kafkaProperties.setProperty("group.id","gp")
    kafkaProperties.setProperty("aoto.offset.reset","earliest")
    val topic = "testTopic"
//    val topicPattern = java.util.regex.Pattern.compile("topic[0-9]")
//    val consumer = new FlinkKafkaConsumer011[String](topicPattern,new SimpleStringSchema(),kafkaProperties)
   val consumer = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),kafkaProperties)
    //指定消費位置
    consumer.setStartFromEarliest()
//    consumer.setStartFromLatest()
//    consumer.setStartFromGroupOffsets()
//    consumer.setStartFromTimestamp(1234567890111L)
//    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
//    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L)
//    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L)
//    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L)
//    consumer.setStartFromSpecificOffsets(specificStartOffsets)
    //设置checkpoint，实现容错
    env.enableCheckpointing(5000L)
    val kafkads: DataStream[String] = env.addSource(consumer)
    //transformation operator
    val mapds: DataStream[String] = kafkads.map(_+"fix")
    //数据结果的sink
    mapds.print()
    //触发执行
    env.execute("sourceDemo")

  }

}
