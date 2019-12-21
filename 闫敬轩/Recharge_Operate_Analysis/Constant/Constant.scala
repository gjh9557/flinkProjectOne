package Recharge_Operate_Analysis.Constant

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/17 14:53
  * @version 1.0
  */
object Constant{
  val DB_URL ="jdbc:mysql://127.0.0.1:3306/Recharge?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
  val DB_USERNAME="root"
  val DB_PASSWORD="123321"
  val DB_RECHARE_DISTRIBUTION = "recharge_distribution"

  val JEDIS_MAX_IDLE = 100
  val JEDIS_MAX_TOTAL = 300
  val JEDIS_MIN_IDLE = 1
  val JEDIS_TIME_OUT = 30000
  val JEDIS_HOST = "hadoop04"
  val JEDIS_PORT = 6379
  val JEDIS_ON_BORROW = true
  val JEDIS_RECHARGE_OVERVIEW = "rechargeOverview"



  case class LogEntry(
                       bussinessRst: String,
                       channelCode: String,
                       chargefee: String,
                       clientIp: String,
                       gateway_id: String,
                       interFacRst: String,
                       logOutTime: String,
                       orderId: String,
                       payPhoneNo: String,
                       phoneno: String,
                       provinceCode: String,
                       rateoperateid: String,
                       receiveNotifyTime: String,
                       requestId: String,
                       retMsg: String,
                       serverIp: String,
                       serverPort: String,
                       serviceName: String,
                       shouldfee: String,
                       srcChannel: String,
                       sysId: String
                     )
  case class Log(
                  serviceName:String,
                  bussinessRst:String,
                  chargefee:String,
                  requestId:String,
                  receiveNotifyTime: String,
                  provinceCode:String
                )
  case class City(
                   provinceCode:String,
                   provinceName:String
                 )
}
