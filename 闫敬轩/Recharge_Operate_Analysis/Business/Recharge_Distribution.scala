package Recharge_Operate_Analysis.Business

import Recharge_Operate_Analysis.Constant.Constant.{Log, LogEntry}
import Recharge_Operate_Analysis.Utils.{KafKaUtils, RechargeDistributionHelper}
import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/18 15:17
  * @version 1.0
  */
object Recharge_Distribution {
  def main(args: Array[String]): Unit = {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    var data = env.addSource(KafKaUtils.getConsumer())
    val loged: DataStream[Log] = data.map(line => {
      val entry: LogEntry = JSON.parseObject(line, classOf[LogEntry])
      Log(entry.serviceName, entry.bussinessRst, entry.chargefee,
        entry.requestId, entry.receiveNotifyTime, entry.provinceCode)
    })
    val res: DataStream[(Int, Int,String)] = loged
      .filter(line => line.serviceName.equals("reChargeNotifyReq"))
      .map(line => {
      val hour = line.receiveNotifyTime.substring(8,10)
      (line.chargefee.toInt, 1,hour)
    })
      .keyBy(_._3)
      .timeWindowAll(Time.seconds(10))
      .reduce((a,b) => (a._1+b._1,a._2+b._2,a._3))
    res.print()

    res.addSink(RechargeDistributionHelper)

    env.execute()

  }
}
