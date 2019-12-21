package flink.product


import java.io.{BufferedReader, File, FileReader}

import flink.common.Constant
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/*
@author Yuniko
2019/12/17
*/

/**
  *
  * 获取UserBehavior文件写入Kafka 制造实时流
  *
  * 制造数据流 写入Kafka
  *
  */
object MakeDataStream_UserBehavior {

  def main(args: Array[String]): Unit = {
    val properties = Constant.getProperties()
    val producer = new KafkaProducer[String, String](properties)
    var reader: BufferedReader = null

    try {
      reader = new BufferedReader(new FileReader(new File(Constant.USER_BEHAVIOR_URL)))
      var line = ""

      while ((line = reader.readLine()) != null) {
        Thread.sleep(10)
        producer.send(new ProducerRecord[String, String](Constant.HOT_TOPIC, line));

      }
    } catch {
      case _: Exception => println("文件读取失败 ")

    } finally {
      if (reader != null)
        reader.close()
      if (producer != null)
        producer.close()
    }


    println("发送完毕")

  }

}
