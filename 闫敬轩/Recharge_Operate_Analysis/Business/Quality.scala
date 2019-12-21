package Recharge_Operate_Analysis.Business

import Recharge_Operate_Analysis.Constant.Constant.{City, Log, LogEntry}
import Recharge_Operate_Analysis.Utils.{FailureCntHelper, KafKaUtils}
import com.alibaba.fastjson.JSON
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, JoinedStreams, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Description:
  * Copyright (c),2019,JingxuanYan
  * This program is protected by copyright laws.
  *
  * @author 闫敬轩
  * @date 2019/12/17 16:55
  * @version 1.0
  */
object Quality {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data = env.addSource(KafKaUtils.getConsumer())
    val city = env.readTextFile("D:\\XSQ-BigData-24\\项目\\充值平台实时统计分析/city.txt")
    val cityDF: DataStream[City] = city.map(line => {
      val splited = line.split("\\s")
      City(splited(0), splited(1))
    })
    val loged: DataStream[Log] = data.map(line => {
      val entry: LogEntry = JSON.parseObject(line, classOf[LogEntry])
      Log(entry.serviceName, entry.bussinessRst, entry.chargefee,
        entry.requestId, entry.receiveNotifyTime, entry.provinceCode)
    })
    val joined: DataStream[(String, String)] = loged
      .filter(line => line.serviceName.equals("reChargeNotifyReq"))
      .join(cityDF).where(_.provinceCode).equalTo(_.provinceCode)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .apply((Log, City) => {
        (Log.bussinessRst, City.provinceName)
      })
//    joined.print()
    val sumed: DataStream[(String, Int)] = joined
      .filter(!_._1.equals("0000"))
      .map(line => (line._2, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce((a, b) => (a._1, a._2 + b._2))
//    val sumed: DataStream[(String, Int)] = loged.filter(!_.bussinessRst.equals("0000"))
//      .map(line => {(line.provinceCode, 1)})
//      .keyBy(_._1)
//      .timeWindow(Time.seconds(10))
//      .reduce((a, b) => (a._1, a._2 + b._2))

    sumed.print()

    sumed.addSink(FailureCntHelper)

    env.execute(this.getClass.getName)

  }
}
