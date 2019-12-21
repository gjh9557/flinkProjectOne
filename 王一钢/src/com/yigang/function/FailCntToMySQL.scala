package com.yigang.function

import java.sql.{Connection, DriverManager}

import com.yigang.utils.DBCPUtil
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * Time：2019-12-19 17:01
  * Email： yiigang@126.com
  * Desc：存入mysql表
  *
  * @author： 王一钢
  * @version：1.0.0
  */
object FailCntToMySQL extends RichSinkFunction[Tuple2[String,Int]]{

  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    val conn: Connection = DBCPUtil.getConnection
    val sql = "INSERT into failcnt(pro,cnt) values(?,?) on duplicate key update cnt=?"
    val statement = conn.prepareStatement(sql)
    statement.setString(1,value._1.toString)
    statement.setInt(2,value._2)
    statement.setInt(3,value._2)
    statement.executeUpdate()

    statement.close()
    conn.close()
  }


}
