package com.qf.recharge.notice

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.qf.recharge.notice.BusinessOverview.ReChargeNotifyReq
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * Description：
  * Copyright (c) ，2019 ， zzy 
  * This program is protected by copyright laws. 
  * Date： 2019年12月19日 
  *
  * @author 张振勇
  * @version : 0.1
  */
object ServiceQuality {
  def main(args: Array[String]): Unit = {
    //1.flink 上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.setParallelism(1)

    //source
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","mini:9092")
    properties.setProperty("group.id","gp24")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.setProperty("auto.offset.reset","latest")

    val data = env.addSource( new FlinkKafkaConsumer011[String]("recharge", new SimpleStringSchema(), properties))
    val city = env.readTextFile("data/city.txt")
      .map(data => {
        val datasplited = data.split("\\s")
        val code = datasplited(0)
        val name = datasplited(1)
        (code, name)
      })
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

    //统计每小时各个省份的充值失败数据量
    val failedNums = rechargeNotifyReq
      .map(data => {
        (data.proviceCode + ":" + data.day + data.hour, if (data.bussinessRst.equals("0000")) 0 else 1)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .reduce((x, y) => (x._1, x._2 + y._2))

    val res = failedNums.join(city)
        .where(failed => failed._1.split(":")(0))
        .equalTo(city => city._1)
        .window(SlidingProcessingTimeWindows.of(Time.hours(1),Time.seconds(10)))
        .apply((failed,city)=>{
          val name = city._2
          val hour = failed._1.split(":")(1)
          (name+":"+hour,failed._2)
        })
    //sink
//    failedNums.print()
    res.addSink( new MyJdbcSink() )
    //execute
    env.execute()
  }
}

class MyJdbcSink extends RichSinkFunction[(String, Int)]{
  //定义sql连接,预编译语句
  var conn: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _

  //初始化,创建链接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://mini:3306/test","root","Mysql@123")
    insertStatement = conn.prepareStatement("insert into recharge_fail (proAndHour, cnt) values (?,?)")
    updateStatement = conn.prepareStatement("update recharge_fail set cnt=? where proAndHour=?")
  }

  //执行
  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStatement.setString(2,value._1)
    updateStatement.setInt(1,value._2)
    updateStatement.execute()
    //如果update没有查到数据,那么执行插入
    if(updateStatement.getUpdateCount==0){
      insertStatement.setString(1,value._1)
      insertStatement.setInt(2,value._2)
      insertStatement.execute()
    }
  }

  override def close(): Unit = {
    insertStatement.close()
    updateStatement.close()
    conn.close()
  }
}