package com.yigang.flink.release1

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.yigang.test.Source_Demo
import com.yigang.utils.{DateUtil, JedisUtil}
import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * Time：2019-12-18 15:21
  * Email： yiie315@163.com
  * Desc：需求一：
  *               统计全网的充值订单量, 充值金额, 充值成功数
  *              	实时充值业务办理趋势, 主要统计全网的订单量数据
  * @author： 王一钢
  * @version：1.0.0
  */
object ChargeOrder {
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
    val base_data: DataStream[(String, String, Int, Int, BigDecimal, Long)] = data
      .filter(new FilterFunction[String] {
        override def filter(t: String): Boolean = {
          //过滤出只是调用充值服务接口的数据
          return JSON.parseObject(t).getString("serviceName").equals("reChargeNotifyReq")
        }
      }).map(line => {
      val serviceName: String = JSON.parseObject(line).getString("serviceName")
      val bussinessRst: String = JSON.parseObject(line).getString("bussinessRst")
      val chargefee1: Double = JSON.parseObject(line).getDouble("chargefee")
      val requestId: String = JSON.parseObject(line).getString("requestId")
      val receiveNotifyTime: String = JSON.parseObject(line).getString("receiveNotifyTime")
      val provinceCode: Int = JSON.parseObject(line).getInteger("provinceCode")

      val chargeTime1: Long = DateUtil.caculateRqt(requestId, receiveNotifyTime) //充值花费时间 结束/开始时间为null时为0
      //充值成功标志 成功为 1  失败为 0
      val tag = if (bussinessRst.equals("0000") && chargeTime1 != 0L) 1 else 0
      //充值成功的充值花费时间
      val chargeTime: Long = if (bussinessRst.equals("0000")) DateUtil.caculateRqt(requestId, receiveNotifyTime) else 0
      //充值成功的充值金额
      val chargefee:BigDecimal = if (tag == 1) chargefee1 else 0L
      val day = receiveNotifyTime.substring(0, 8)
      val hour = receiveNotifyTime.substring(8, 10)
      //日期 小时 省份代码 结果代码  充值金额 充值时长(毫秒)）
      (day, hour, provinceCode, tag, chargefee, chargeTime)
    })

    /**
      * 需求一（1）
      *   结果：(20170412,13679,13659,99.85%,100431348.0元,19.30秒)
      */
    val res1: DataStream[(String, Int, Int, String, String, String)] =
    //日期 结果代码 充值金额 充值花费时间 1 1.00 1.00
      base_data.map(x => (x._1, x._4, x._5, x._6, 1, 1.00, 1.00))
      .timeWindowAll(Time.seconds(10L))
      //日期 成功数 充值金额 总充值时间 充值总数 成功率 平均充值时间
      .reduce((x, y) => (x._1, x._2 + y._2, x._3 + y._3, (x._4 + y._4), x._5 + y._5, (x._2 + y._2) * 1.0 / (x._5 + y._5), (x._4 + y._4) * 1.00 / (x._2 + y._2)))
      //日期 充值总数 充值成功数 成功率 充值金额 平均充值时间
      .map(x => (x._1, x._5, x._2, (x._6 * 100).formatted("%.2f") + "%", x._3 + "元", (x._7 / 1000).formatted("%.2f") + "秒"))

    /**
      * 需求一（2）
      *   结果：
      *   3> (20170412,03,443,443,100.0)
      *   2> (20170412,07,7885,7882,99.96)
      *   3> (20170412,08,1267,1266,99.92)
      *   3> (20170412,06,2769,2769,100.0)
      *   1> (20170412,04,453,453,100.0)
      *   1 > (20170412,05,854,854,100.0)
      */
    val res2: DataStream[(String, String, Int, Int, Double)] = base_data.map(x => (x._1, x._2, x._4, 1, 1.0)) // 日期 小时 tag 1
      .keyBy(x => (x._1, x._2)) //按照 日期和小时分区
      //每5秒统计过去一小时结果
      .timeWindow(Time.hours(1), Time.seconds(5))
      // 日期 月份 充值总数 成功数 成功率
      .reduce((x, y) => (x._1, x._2, (x._4 + y._4).toInt, x._3 + y._3, ((x._3 + y._3) * 1.0 / (x._4 + y._4) * 100).formatted("%.2f").toDouble))

    //结果一存入redis
    res1.addSink(x=>{
      val jedis = JedisUtil.getJedisClient()
      val map = new util.LinkedHashMap[String,String]();
      map.put("日期", x._1);
      map.put("充值总数", x._2.toString);
      map.put("充值成功数", x._3.toString);
      map.put("成功率", x._4);
      map.put("充值金额", x._5);
      map.put("平均充值时间", x._6);
      jedis.hmset("release01_res1", map);
      JedisUtil.releaseJedis(jedis)
    })

    //结果二存入redis
    res2.addSink(x=>{
      val jedis = JedisUtil.getJedisClient()
      jedis.lpush(x._2+"月份","充值总数: "+x._3,"成功数: "+x._4,"成功率: "+x._5)
      JedisUtil.releaseJedis(jedis)
    })

    env.execute()
  }
}
