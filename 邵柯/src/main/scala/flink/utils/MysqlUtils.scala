package flink.utils

import flink.business.HourTotal
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/*
@author Yuniko
2019/12/18
*/


object MysqlUtils {

  DBs.setup()

  def saveRate(provinceCode:String,rate:Double): Unit = {


    /**
      *
      * Scalike 实现插入 如果存在就更新
      *
      */
    DB.localTx(implicit sessioin => {
      //如果存在组件不需要指定
      SQL("insert into rate(provinceCode,rate) values(?,?) on  DUPLICATE key update rate=values(rate)")
        .bind(provinceCode, rate)
        .update().apply()
    })
  }
  def saveHourTotal(hourTotal:HourTotal): Unit ={

    // (Hour:String,count:Long,money:Long)
    DB.localTx(implicit sessioin => {
      //如果存在组件不需要指定
      SQL("insert into hourtotal(hour,count,money) values(?,?,?) on  DUPLICATE key update count = values(count) ,money = values(money)")
        .bind(hourTotal.Hour,hourTotal.count,hourTotal.money)
        .update().apply()
    })
  }
  def main(args: Array[String]): Unit = {
   //saveHourTotal(HourTotal("90",2,3))
   saveHourTotal(HourTotal("90",1,1))
  }
}
