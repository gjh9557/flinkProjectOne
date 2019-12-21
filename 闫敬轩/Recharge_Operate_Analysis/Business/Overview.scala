package Recharge_Operate_Analysis.Business

import Recharge_Operate_Analysis.Constant.Constant.{Log, LogEntry}
import Recharge_Operate_Analysis.Utils.{KafKaUtils, RedisUtils}
import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/16 17:05
  * @version 1.0
  */
object Overview {
  def main(args: Array[String]): Unit = {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    var data = env.addSource(KafKaUtils.getConsumer())
    val loged: DataStream[Log] = data.map(line => {
      val entry: LogEntry = JSON.parseObject(line, classOf[LogEntry])
      Log(entry.serviceName, entry.bussinessRst, entry.chargefee,
        entry.requestId, entry.receiveNotifyTime, entry.provinceCode)
    })
    val maped: DataStream[(Int, Int, Int, Double,String,String)] = loged
      .filter(line => line.serviceName.equals("reChargeNotifyReq"))
      .map(line => {
        val day = line.receiveNotifyTime.substring(0,8)
        val hour = line.receiveNotifyTime.substring(8,10)
      if (line.bussinessRst.equals("0000"))
        (line.chargefee.toInt, 1, 1, 0,day,hour)
      else (line.chargefee.toInt, 1, 0, 0,day,hour)
    })
    val res: DataStream[(Int, Int, Int, Double,String,String)] = maped
        .keyBy(x => (x._5,x._6))
      .timeWindowAll(Time.seconds(10))
      .reduce((a,b) =>(a._1 + b._1, a._2 + b._2, a._3 + b._3,(a._3+b._3)*1.0/(a._2+b._2),a._5,a._6))

    res.print()

    res.addSink(x =>{
      val jedis = RedisUtils.getPool
      jedis.lpush(x._5+"小时","充值金额:" +x._1,
        "充值总数："+x._2,"充值成功数"+x._3,"成功率"+x._4)
      jedis.close()
    })

    env.execute(this.getClass.getName)


  }
}
