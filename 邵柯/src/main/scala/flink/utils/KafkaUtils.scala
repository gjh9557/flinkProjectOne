package flink.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/*
@author Yuniko
2019/12/17
*/

object KafkaUtils {
  val properties = new Properties
  properties.setProperty("bootstrap.servers", "node245:9092")
  properties.setProperty("group.id", "flink")
  properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("auto.offset.reset", "latest")
  properties

  def getKafkaProperties(): Properties = {
    properties
  }

  def getKafkaSource(topic:String): FlinkKafkaConsumer011[String] = {
    val KafkaSourceConsumer=  new FlinkKafkaConsumer011[String] (topic,new SimpleStringSchema(),properties)
    KafkaSourceConsumer
  }
}
