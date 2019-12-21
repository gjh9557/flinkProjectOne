package Recharge_Operate_Analysis.Business

import Recharge_Operate_Analysis.Constant.Constant.{City, Log, LogEntry}
import Recharge_Operate_Analysis.Utils.FailureTop3Helper
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/18 15:19
  * @version 1.0
  */
object FailureTop3 {
  def main(args: Array[String]): Unit = {
    var env = ExecutionEnvironment.getExecutionEnvironment
    var data = env.readTextFile("D:\\XSQ-BigData-24\\项目\\充值平台实时统计分析/cmcc.json")
    import org.apache.flink.api.scala._
    val loged: DataSet[Log] = data.map(line => {
      val entry: LogEntry = JSON.parseObject(line, classOf[LogEntry])
      Log(entry.serviceName, entry.bussinessRst, entry.chargefee,
        entry.requestId, entry.receiveNotifyTime, entry.provinceCode)
    })
    var city = env.readTextFile("D:\\XSQ-BigData-24\\项目\\充值平台实时统计分析/city.txt")
    val cityDF: DataSet[City] = city.map(line => {
      val splited = line.split("\\s")
      City(splited(0), splited(1))
    })
    val maped: DataSet[(String, Int, Int)] = loged
      .filter(line => line.serviceName.equals("reChargeNotifyReq"))
      .join(cityDF).where(_.provinceCode).equalTo(_.provinceCode)
      .map(line => {
        if (!line._1.bussinessRst.equals("0000"))
          (line._2.provinceName, 1, 1)
        else
          (line._2.provinceName, 1, 0)
      })
    val redueced: DataSet[(String, Int, Int)] = maped.groupBy(_._1)
      .reduce((a, b) => (a._1, a._2 + b._2, a._3 + b._3))
    val res: DataSet[(String, Int, Double)] = redueced
      .map(line => {
        (line._1, line._3,  line._3 * 1.0/ line._2 )
      }).sortPartition(1,Order.DESCENDING)
      .first(3)

    res.map(x => {
      FailureTop3Helper.invokeTopN(x._1,x._2,x._3)
      FailureTop3Helper.close()
    })

    res.print()
  }
}
