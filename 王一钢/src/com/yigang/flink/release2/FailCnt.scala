package com.yigang.flink.release2

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.yigang.function.FailCntToMySQL
import com.yigang.test.Source_Demo
import com.yigang.utils.DateUtil
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * Time：2019-12-19 14:55
  * Email： yiigang@126.com
  * Desc：需求二：
  *             全国各省充值业务失败量分布
  *             统计每小时各个省份的充值失败数据量
  *
  * @author： 王一钢
  * @version：1.0.0
  */
object FailCnt {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //从kafka中实时拉取数据
    val pro = new Properties()
    pro.load(Source_Demo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    val consumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("pay1",new SimpleStringSchema(),pro)
    consumer.setStartFromEarliest()
    val data: DataStream[String] = env.addSource(consumer)

    //取出json中用到的字段并做简单处理
    val base_data: DataStream[(String, String, Int, Int)] = data
      .filter(new FilterFunction[String] {
        override def filter(t: String): Boolean = {
          //过滤出只是调用充值服务接口的数据
          return JSON.parseObject(t).getString("serviceName").equals("reChargeNotifyReq")&&
            (!"0000".equals( JSON.parseObject(t).getString("bussinessRst")) ||
              JSON.parseObject(t).getString("receiveNotifyTime") == null ||
              "".equals(JSON.parseObject(t).getString("receiveNotifyTime")))
        }
      }).map(line => {
      val bussinessRst: String = JSON.parseObject(line).getString("bussinessRst")
      val receiveNotifyTime: String = JSON.parseObject(line).getString("receiveNotifyTime")
      val provinceCode: Int = JSON.parseObject(line).getInteger("provinceCode")


      val day = receiveNotifyTime.substring(0, 4)
      val hour = receiveNotifyTime.substring(8, 10)
      //日期 小时 省份代码 失败标志）
      (day, hour, provinceCode, 1)
    })

    //省份代码和失败次数
    val pro_cnt: DataStream[(Int, Int)] = base_data
      .map(x => (x._3, x._4)) //省份代码  失败标志
      .keyBy(0)
      .timeWindow(Time.seconds(1))
      .reduce((x, y) => (x._1, x._2 + y._2))

    //读取省份映射表
    val city: DataStream[String] = env.readTextFile("data/flink/city.txt")


    /**
      * 结果：
      * 1> (黑龙江,2)
      * 2> (广东,17)
      * 1> (新疆,1)
      */
    //进行join
    val res1: DataStream[(String, Int)] = city.map(x => {
      val line = x.split("\\s")
      val num = line(0).toInt
      val pro = line(1)
      (num, pro)
    }).join(pro_cnt)
      .where(x => x._1)
      .equalTo(x => x._1)
      //每5秒统计过去1小时内的失败量
      .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(5)))
      .apply((x, y) => (x._2, y._2))

    res1.addSink(FailCntToMySQL)

    env.execute()
  }
}
