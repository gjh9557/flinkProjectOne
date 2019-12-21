package spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
@author Yuniko
2019/12/17
*/


object OffsetMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val groupid ="spark"
    val brokerList = "hadoop1:9092,hadoop2:9092,hadoop3:9092"
    val topic = "test"
    val topics = Set(topic)
/*    val Kafkas  = Map(
      "metadata.broker.list"-> brokerList,
      "group,id" -> groupid,
      "auto.offset.rest"->kafka.


    )*/


  }
}
