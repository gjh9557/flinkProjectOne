package com.qf.recharge.notice

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Description：
  * Copyright (c) ，2019 ， zzy 
  * This program is protected by copyright laws. 
  * Date： 2019年12月21日 
  *
  * @author 张振勇
  * @version : 0.1
  */
object ProvinceTopN {
  def main(args: Array[String]): Unit = {
    //1.flink 上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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
        val receiveNotifyTime = data.getString("receiveNotifyTime")
        val provinceCode = data.getString("provinceCode")
        val bussinessRst = data.getString("bussinessRst")
        val cnt = 1
        val successCnt = if(bussinessRst.equals("0000")) 1 else 0
        val timeStamp = simpleDateFormat.parse(receiveNotifyTime).getTime
        val successRate = 1.0
        (provinceCode, cnt, successCnt, timeStamp,successRate)
      })
        .assignAscendingTimestamps(_._4)

    val res = rechargeNotifyReq.keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .reduce((x, y) => {
        val provinceCode = x._1
        val cnt = x._2 + y._2
        val successCnt = x._3 + y._3
        val timeStamp = y._4
        val rate = successCnt / cnt
        (provinceCode, cnt, successCnt, timeStamp, rate.formatted("%1.f").toDouble)
      })
      .map(x => ProvinceCnt("1",x._1, x._2, x._4,x._5))
      .keyBy(_.key)
      .process(new TopN(10))
    //sink
    res.addSink( new MyJdbcSink2() )
    //execute
    env.execute()
  }
}

class TopN(totalSize: Int) extends KeyedProcessFunction[String, ProvinceCnt, String] {
  private var Top:ListState[ProvinceCnt] = _

  override def open(parameters: Configuration): Unit = {
    Top = getRuntimeContext.getListState( new ListStateDescriptor[ProvinceCnt]("top-state", classOf[ProvinceCnt]))
  }

  override def processElement(value: ProvinceCnt, ctx: KeyedProcessFunction[String, ProvinceCnt, String]#Context, out: Collector[String]): Unit = {
    Top.add(value)
    ctx.timerService().registerEventTimeTimer(value.timestamp + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, ProvinceCnt, String]#OnTimerContext, out: Collector[String]): Unit = {
    val all:ListBuffer[ProvinceCnt] = new ListBuffer[ProvinceCnt]
    import scala.collection.JavaConversions._
    for(i <- Top.get()){
      all += i
    }
    val sorted = all.sortBy(_.cnt)(Ordering.Int.reverse).take(totalSize)
    Top.clear()
    val result: StringBuilder = new StringBuilder
    for(i <- sorted.indices){
      val current = sorted(i)
      result.append(i+1)
        .append("+").append(current.province)
        .append("+").append(current.cnt)
        .append("+").append(current.rate)
        .append("&")
    }
    out.collect(result.toString())
  }
}

case class ProvinceCnt(key:String, province:String, cnt:Int, timestamp:Long, rate:Double)

class MyJdbcSink2 extends RichSinkFunction[String]{
  //定义sql连接,预编译语句
  var conn: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _

  //初始化,创建链接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://mini:3306/test","root","Mysql@123")
    insertStatement = conn.prepareStatement("insert into recharge_top (rank, province, cnt,rate) values (?,?,?,?)")
    updateStatement = conn.prepareStatement("update recharge_top set province=?, cnt=?, rate=? where rank=?")
  }

  //执行
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val splited = value.split("&")
    if(splited.nonEmpty){
      for(i <- splited.indices){
        val splited2 = splited(i).split("+")
        if(splited2.nonEmpty) {
          val rank = splited2(0).trim.toInt
          val province = splited2(1)
          val cnt = splited2(2).trim.toInt
          val rate = splited2(3).trim.toDouble
          //执行更新语句
          updateStatement.setString(1,province)
          updateStatement.setInt(2,cnt)
          updateStatement.setDouble(3,rate)
          updateStatement.setInt(4, rank)
          updateStatement.execute()
          //如果update没有查到数据,那么执行插入
          if(updateStatement.getUpdateCount==0){
            insertStatement.setInt(1,rank)
            insertStatement.setString(2,province)
            insertStatement.setInt(3,cnt)
            updateStatement.setDouble(4,rate)
            insertStatement.execute()
          }
        }
      }
    }

  }

  override def close(): Unit = {
    insertStatement.close()
    updateStatement.close()
    conn.close()
  }
}