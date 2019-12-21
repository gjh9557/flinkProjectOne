package flink.business

import com.alibaba.fastjson.JSON
import flink.common.Constant
import flink.utils.{FlinkUtils, RedisUtils}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/*
@author Yuniko
2019/12/18
*/
/**
  * 简易实现需求1
  * 使用Reduce来实现
  *
  *
  */
object Need1_Bak {
  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getEnv()
    // val source = env.addSource(KafkaUtils.getKafkaSource("cmcc"))
    val source = env.readTextFile(Constant.CMCC_URL)
    source.setParallelism(1)
    //使用fastjson解析用到参数
    val dataSource = source.map(line => {

      val jsonObj = JSON.parseObject(line)

      val serviceName = jsonObj.getString("serviceName")
      val bussinessRst = jsonObj.getString("bussinessRst")
      val chargefee = jsonObj.getString("chargefee")

      val requestTime = if (jsonObj.getString("requestTime") != null) jsonObj.getString("requestTime").toLong else 1
      val receiveNotifyTime = if (jsonObj.getString("receiveNotifyTime") != null) jsonObj.getString("receiveNotifyTime").toLong else 1
      val provinceCode = jsonObj.getString("provinceCode")

      CMCC(serviceName, bussinessRst, chargefee, requestTime, receiveNotifyTime, provinceCode)

    }).map(cmcc => {
      Total_Bak(1, cmcc.chargefee.toLong, 1, cmcc.resultTime - cmcc.receiveNotifyTime, cmcc.provinceCode)
    }).keyBy(_.provinceCode).reduce((cmccx, cmccy) => {
      val moeyTotal = cmccx.moneyTotal + cmccy.moneyTotal
      val orderSize = cmccx.orderSize + cmccy.orderSize
      val successTotal = cmccx.successTotal + cmccy.successTotal
      val TimeTotal = cmccx.TimeTotal + cmccy.TimeTotal
      val provinceCode = cmccy.provinceCode
      Total_Bak(orderSize, moeyTotal, successTotal, TimeTotal, provinceCode)
    })
    dataSource.print("Need1")
    dataSource.addSink(new MyRedisSink)
    env.execute("Need")

  }
}

case class Total_Bak(orderSize: Long, moneyTotal: Long, successTotal: Long, TimeTotal: Long, provinceCode: String)

class MyRedisSink extends  RichSinkFunction[Total_Bak]{
  override def invoke(value: Total_Bak, context: SinkFunction.Context[_]): Unit = {

    RedisUtils.saveNetworkInfo(value)
  }
}