import com.alibaba.fastjson.JSON

/*
@author Yuniko
2019/12/18
*/ object jsonTest {
  def main(args: Array[String]): Unit = {
    val s = "{\"bussinessRst\":\"0000\",\"channelCode\":\"0705\",\"chargefee\":\"10000\",\"clientIp\":\"117.63.47.231\",\"endReqTime\":\"20170412071838537\",\"idType\":\"01\",\"interFacRst\":\"0000\",\"logOutTime\":\"20170412071838537\",\"orderId\":\"384679097178722761\",\"prodCnt\":\"1\",\"provinceCode\":\"230\",\"requestId\":\"20170412071817807600679071040000\",\"retMsg\":\"成功\",\"serverIp\":\"172.16.59.241\",\"serverPort\":\"8088\",\"serviceName\":\"sendRechargeReq\",\"shouldfee\":\"9950\",\"startReqTime\":\"20170412071838431\",\"sysId\":\"15\"}"
    val jsonObj = JSON.parseObject(s)
    val serviceName = jsonObj.getString("serviceName")
    val bussinessRst = jsonObj.getString("bussinessRst")
    val chargefee = jsonObj.getString("chargefee")

    val requestTime = if (jsonObj.getString("requestTime") != null) jsonObj.getString("requestTime").toLong else 1
    val receiveNotifyTime = if (jsonObj.getString("receiveNotifyTime") != null) jsonObj.getString("receiveNotifyTime").toLong else 1
    val provinceCode = jsonObj.getString("provinceCode")

    // println(requestTime +" "+provinceCode)

    val yu = 123.223
    val form = yu.formatted("%.2f")
    val ysu = "20170412030030017"
    //println(ysu.substring(8,10))
    //println(form)
    val line = "83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png"
    val regex = "(.*) - - (.*)+0000(.*) (.*)$".r
    // ip:String,userId:String,eventTime:Long,method:String,url:String

    val (ip: String, eventTime: String, method: String, url: String) = line match {
      case regex(x, y, z, d) => (x, y, z, d)
    }
    println(ip + " " + method)

  }
}
