package flink.business

import com.alibaba.fastjson.JSON
import flink.common.Constant
import flink.utils.MysqlUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
/*
@author Yuniko
2019/12/18
*/

/**
  *  4.实时充值情况分布（存入MySQL）
  * 实时统计每小时的充值笔数和充值金额。
  *
  */
object Need4 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source = env.readTextFile(Constant.CMCC_URL)
    val hourContext = source.map(line => {
      val jsonObj = JSON.parseObject(line)
      //20170412030030017
      val time = jsonObj.getString("receiveNotifyTime")
      var hour = "00"
      val chargefee = jsonObj.getString("chargefee").toLong
      if (time!=null &&time.length>11)
        hour = time.substring(8, 10)
      HourTotal(hour, 1, chargefee)

    }).keyBy(_.Hour)
      .reduce((xHourTotal, yHourTotal) => {
        HourTotal(xHourTotal.Hour, xHourTotal.count + yHourTotal.count, xHourTotal.money + yHourTotal.money)
      })
    hourContext.print("Content")
    hourContext.addSink(new MyMysqlSink)
    env.execute("Need4")
  }

}
case class HourTotal(Hour:String,count:Long,money:Long)
class MyMysqlSink extends  SinkFunction[HourTotal]{
  override def invoke(value: HourTotal, context: SinkFunction.Context[_]): Unit = {

   MysqlUtils.saveHourTotal(value)

  }
}