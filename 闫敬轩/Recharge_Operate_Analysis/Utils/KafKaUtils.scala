package Recharge_Operate_Analysis.Utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object KafKaUtils {
  def getConsumer()= {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val pro = new Properties()
    pro.setProperty("zookeeper.connect","hadoop04:2181")
    pro.setProperty("bootstrap.servers","hadoop04:9092")
    pro.setProperty("group.id","test-consumer-group")
    pro.setProperty("aoto.offset.reset","earliest")
//    pro.load(Source_Demo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    val consumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("recharge",new SimpleStringSchema(),pro)
    consumer.setStartFromEarliest()
    consumer
//    val data: DataStream[String] = env.addSource(consumer)
//    data.print()
//    env.execute()
//    data
  }

}
