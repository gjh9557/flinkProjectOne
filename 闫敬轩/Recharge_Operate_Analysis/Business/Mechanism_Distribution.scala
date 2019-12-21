package Recharge_Operate_Analysis.Business

import Recharge_Operate_Analysis.Constant.Constant.{City, Log, LogEntry}
import Recharge_Operate_Analysis.Utils.KafKaUtils
import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.scala.{DataStream, JoinedStreams, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/18 15:26
  * @version 1.0
  */
object Mechanism_Distribution {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    var data = env.addSource(KafKaUtils.getConsumer())
    val loged: DataStream[Log] = data.map(line => {
      val entry: LogEntry = JSON.parseObject(line, classOf[LogEntry])
      Log(entry.serviceName, entry.bussinessRst, entry.chargefee,
        entry.requestId, entry.receiveNotifyTime, entry.provinceCode)
    })
    var city = env.readTextFile("D:\\XSQ-BigData-24\\项目\\充值平台实时统计分析/city.txt")
    val cityDF: DataStream[City] = city.map(line => {
      val splited = line.split("\\s")
      City(splited(0), splited(1))
    })

    val reduced: DataStream[(String, Int, Int)] = loged
      .filter(line => line.serviceName.equals("reChargeNotifyReq"))
      .join(cityDF)
      .where(_.provinceCode)
      .equalTo(_.provinceCode)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .apply((Log,City) => {
        (City.provinceName,Log.chargefee.toInt,1)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((a, b) => (a._1, a._2 + b._2, a._3 + b._3))
    val to: JoinedStreams[(String, Int, Int), City]#Where[String]#EqualTo = reduced.join(cityDF).where(_._1).equalTo(_.provinceCode)
        
    reduced.print()


    env.execute()
  }
}
