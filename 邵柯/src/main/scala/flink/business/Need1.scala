package flink.business

import java.util.Date

import com.alibaba.fastjson.JSON
import flink.common.Constant
import flink.utils.{FlinkUtils, KafkaUtils}
import javax.swing.SwingWorker.StateValue
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/*
@author Yuniko
2019/12/17
*/

/**
  *
  * 1.业务概况---必做（结果存入Redis）
  *
  * 1)统计全网的充值订单量, 充值金额, 充值成功数
  *
  * 2)实时充值业务办理趋势, 主要统计全网的订单量数据
  *
  *
  *
  */
object Need1 {

  def main(args: Array[String]): Unit = {

    val env = FlinkUtils.getEnv()
    // val source = env.addSource(KafkaUtils.getKafkaSource("cmcc"))
    val source = env.readTextFile(Constant.CMCC_URL)
    source.print("ss")
    //使用fastjson解析用到参数
    val dataSource = source.map(line => {

      val jsonObj = JSON.parseObject(line)
      /**
        * "serviceName"："reChargeNotifyReq"
        * "bussinessRst"： //充值结果
        * "chargefee"：//充值金额
        * "requestTime"： //开始充值时间
        * "receiveNotifyTime"： //结束充值时间
        * "provinceCode"： //获得省份编号
        *
        * "{\"bussinessRst\":\"0000\",\"channelCode\":\"0705\",\
        * "chargefee\":\"10000\",\"clientIp\":\"117.63.47.231\",\"endReqTime\":\"20170412071838537\",\"idType\":\"01\",\"interFacRst\":\"0000\",\"logOutTime\":\"20170412071838537\",\"orderId\":\"384679097178722761\",\"prodCnt\":\"1\",\"provinceCode\":\"230\",\"requestId\":\"20170412071817807600679071040000\",\"retMsg\":\"成功\",\"serverIp\":\"172.16.59.241\",\"serverPort\":\"8088\",\"serviceName\":\"sendRechargeReq\",\"shouldfee\":\"9950\",\"startReqTime\":\"20170412071838431\",\"sysId\":\"15\"}"
        */

      val serviceName = jsonObj.getString("serviceName")
      val bussinessRst = jsonObj.getString("bussinessRst")
      val chargefee = jsonObj.getString("chargefee")

      val requestTime = if (jsonObj.getString("requestTime") != null) jsonObj.getString("requestTime").toLong else 1
      val receiveNotifyTime = if (jsonObj.getString("receiveNotifyTime") != null) jsonObj.getString("receiveNotifyTime").toLong else 1
      val provinceCode = jsonObj.getString("provinceCode")

      CMCC(serviceName, bussinessRst, chargefee, requestTime, receiveNotifyTime, provinceCode)
    })
    dataSource
      .keyBy(_.provinceCode)
      //.filter(_.bussinessRst.equals("reChargeNotifyReq"))
      //.process(new MyProcessFunction())
      .map(new MyMapFunction)
      .print("input")
    env.execute("job")

    /**
      * {"bussinessRst":"0000",
      * "channelCode":"6900",
      * "chargefee":"10500",
      * "clientIp":"116.199.98.166",
      * "gateway_id":"WXPAY",
      * "idType":"01",
      * "interFacRst":"0000",
      * "logOutTime":"20170412030043789",
      * "orderId":"384663608173233879",
      * "phoneno":"13719333435",
      * "provinceCode":"200",
      * "receiveNotifyTime":"20170412030043760",
      * "requestId":"20170412030008471926341864858474",
      * "resultTime":"20170412030043",
      * "retMsg":"接口调用成功",
      * "serverIp":"10.255.254.10",
      * "serverPort":"8714",
      * "serviceName":"reChargeNotifyReq",
      * "sysId":"01"}
      *
      * 需要获取字段
      * "serviceName"："reChargeNotifyReq"
      * "bussinessRst"： //充值结果
      * "chargefee"：//充值金额
      * "resultTime"： //开始充值时间
      * "receiveNotifyTime"： //结束充值时间
      * "provinceCode"： //获得省份编号
      *
      *
      *
      */

  }

}

case class CMCC(serviceName: String, bussinessRst: String, chargefee: String, resultTime: Long, receiveNotifyTime: Long, provinceCode: String)

case class Total(orderSize: Long, moneyTotal: Long, successTotal: Long, TimeTotal: Long)

/**
  *
  *
  * @param < K> Type of the key.
  * @param < I> Type of the input elements.
  * @param < O> Type of the output elements.
  */
class MyProcessFunction() extends KeyedProcessFunction[String, CMCC, Total] {
  private var orderSize: ValueState[Long] = _
  private var moneyTotal: ValueState[Long] = _
  private var successTotal: ValueState[Long] = _
  private var TimeTotal: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {

    orderSize = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("orderSize", classOf[Long]) ) // Look {} 不要加 不然你会报一个想不到的错误
    moneyTotal = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("moneyTotal", classOf[Long]) )
    successTotal = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("successTotal", classOf[Long]) )
    TimeTotal = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("TimeTotal", classOf[Long]) )
  }


  override def processElement(value: CMCC, ctx: KeyedProcessFunction[String, CMCC, Total]#Context, out: Collector[Total]): Unit = {
    orderSize.update(orderSize.value + 1)
    val character = if (value.chargefee != null) value.chargefee.toLong else 0
    moneyTotal.update(moneyTotal.value + character)
    successTotal.update(successTotal.value + 1)
    TimeTotal.update(TimeTotal.value + value.receiveNotifyTime - value.resultTime)
    Total(orderSize.value, moneyTotal.value, successTotal.value, TimeTotal.value)
    //注册定时器10s 写入一次
    ctx.timerService().registerProcessingTimeTimer(new Date().getTime + 10000)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, CMCC, Total]#OnTimerContext, out: Collector[Total]): Unit = {


    println("写入redis")

  }
}

/**
  * 自定义MapFunction
  */
class MyMapFunction extends RichMapFunction[CMCC, Total] {
  // 充值订单量, 充值金额, 充值成功数
  private var orderSize: ValueState[Long] = _
  private var moneyTotal: ValueState[Long] = _
  private var successTotal: ValueState[Long] = _
  private var TimeTotal: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {

    orderSize = getRuntimeContext.getState(new ValueStateDescriptor[Long]("orderSize", classOf[Long]) )
    moneyTotal = getRuntimeContext.getState(new ValueStateDescriptor[Long]("moneyTotal", classOf[Long]) )
    successTotal = getRuntimeContext.getState(new ValueStateDescriptor[Long]("successTotal", classOf[Long]) )
    TimeTotal = getRuntimeContext.getState(new ValueStateDescriptor[Long]("TimeTotal", classOf[Long]) )

  }

  override def map(value: CMCC): Total = {
    orderSize.update(orderSize.value + 1)
    val character = if (value.chargefee != null) value.chargefee.toLong else 0
    moneyTotal.update(moneyTotal.value + character)
    successTotal.update(successTotal.value + 1)
    TimeTotal.update(TimeTotal.value + value.receiveNotifyTime - value.resultTime)
    Total(orderSize.value, moneyTotal.value, successTotal.value, TimeTotal.value)
  }

  override def close(): Unit = {
    orderSize.clear()
    moneyTotal.clear()
    successTotal.clear()
    TimeTotal.clear()


  }
}
