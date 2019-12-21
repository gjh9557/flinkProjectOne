package com.yigang.function

import java.sql.Connection

import com.yigang.utils.DBCPUtil
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * Time：2019-12-21 10:56
  * Email： yiie315@163.com
  * Desc：
  *
  * @author： 王一钢
  * @version：1.0.0
  */
object ProRateToMySQL extends RichSinkFunction[Tuple2[String,String]]{

  override def invoke(value: (String, String), context: SinkFunction.Context[_]): Unit = {
    val conn: Connection = DBCPUtil.getConnection
    val sql = "INSERT into prorate(pro,rate) values(?,?) on duplicate key update rate=?"
    val statement = conn.prepareStatement(sql)
    statement.setString(1,value._1.toString)
    statement.setString(2,value._2)
    statement.setString(3,value._2)
    statement.executeUpdate()

    statement.close()
    conn.close()
  }
}
