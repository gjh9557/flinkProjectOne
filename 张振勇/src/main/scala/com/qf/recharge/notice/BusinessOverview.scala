package com.qf.recharge.notice

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import redis.clients.jedis.Jedis

/**
  * Description：
  * Copyright (c) ，2019 ， zzy 
  * This program is protected by copyright laws. 
  * Date： 2019年12月17日 
  *
  * @author 张振勇
  * @version : 0.1
  */
object BusinessOverview {
  def main(args: Array[String]): Unit = {
    //1.flink 上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //source
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","mini:9092")
    properties.setProperty("group.id","gp24")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.setProperty("auto.offset.reset","latest")

    val data = env.addSource( new FlinkKafkaConsumer011[String]("recharge", new SimpleStringSchema(), properties))

    //transform
    val rechargeNotifyReq = data.map(line => {
      val jsonObject = JSON.parseObject(line)
      (jsonObject)
    }).filter(json => json.getString("serviceName").equals("reChargeNotifyReq"))
      .map(data => {
        val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
        val startTime = data.getString("requestId").substring(0,17)
        val day = startTime.substring(0,8)
        val hour = startTime.substring(8,10)
        val endTime = data.getString("receiveNotifyTime")
        val allchargeTime = simpleDateFormat.parse(endTime).getTime - simpleDateFormat.parse(startTime).getTime
        val provinceCode = data.getString("provinceCode")
        val chargefee = data.getString("chargefee")
        val bussinessRst = data.getString("bussinessRst")
        val chargeTime = if(bussinessRst.equals("0000")) allchargeTime else 0
        //日期,小时,省份编码,充值金额,充值时长,充值结果,计数
        ReChargeNotifyReq(day, hour, provinceCode, chargefee.trim.toDouble, chargeTime, bussinessRst, 1)
      })

    //需求一: 1)统计全网的充值订单量, 充值金额, 充值成功数
    val res1 = rechargeNotifyReq
        .map(data =>{
          //日期,订单计数,订单成功计数,充值金额,充值时长
          val day = data.day
          val cnt = data.cnt
          val successCnt = if(data.bussinessRst.equals("0000")) 1 else 0
          val chargefee = data.chargefee
          val chargeTime = data.chargeTime
          Res1(day, cnt, successCnt, 1.0, chargefee, chargeTime)
        })
        .timeWindowAll( Time.seconds(10))
        .reduce((x, y) => {
          //日期 充值总数 充值成功数 成功率 充值金额 平均充值时间
          val day = x.day
          val totalNums = x.cnt + y.cnt
          val successCnt = x.successCnt + y.successCnt
          val successRate = successCnt * 100.0 / totalNums
          val totalChargeFee = x.chargefee + y.chargefee
          val avgChargeTime = (x.chargeTime + y.chargeTime) / successCnt / 1000.0

          Res1(day, totalNums, successCnt, successRate, totalChargeFee, avgChargeTime.formatted("%.2f").toDouble)
        })
    //2实时充值业务办理趋势, 主要统计全网的订单量数据
    val res2 = rechargeNotifyReq
        .map(data => {
          (data.day + data.hour, data.cnt, if(data.bussinessRst.equals("0000")) 1 else 0)
        })
        .keyBy(_._1)
        .timeWindow(Time.hours(1), Time.seconds(10))
        .reduce((x, y) => (x._1, x._2+y._2, x._3+y._3))
        .map(data => (data._1, data._2, data._3/data._2 * 100 ))

    //sink
    res1.addSink(data => {
      val jedis = new Jedis("mini", 6379, 2000)
      val map = new util.LinkedHashMap[String, String]()
      //日期 充值总数 充值成功数 成功率 充值金额 平均充值时间
      map.put("day", data.day)
      map.put("totalNums", data.cnt.toString)
      map.put("totalSuccessNums",data.successCnt.toString)
      map.put("SuccessRate",data.successRate.toString)
      map.put("totalChargeFee", data.chargefee.toString)
      map.put("avgChargeTime",data.chargeTime.toString)
      jedis.hmset(data.day, map)
      jedis.close()
    })
    res2.addSink(data => {
      val jedis = new Jedis("mini", 6379, 2000)
      val map = new util.LinkedHashMap[String, String]()
      //日期 订单量 成功率
      map.put("time", data._1)
      map.put("totalNums", data._2.toString)
      map.put("successRate",data._3.toString)
      jedis.hmset(data._1, map)
      jedis.close()
    })

    env.execute()
  }
  //日期,小时,省份编码,充值金额,充值时长,充值结果
  case class ReChargeNotifyReq( day: String, hour: String, proviceCode: String, chargefee: Double, chargeTime: Long, bussinessRst: String, cnt: Long)
  //日期,订单计数,订单成功计数, 成功率 ,充值金额,充值时长
  case class Res1( day: String, cnt: Long, successCnt: Long, successRate: Double, chargefee: Double, chargeTime: Double)

}

