package com.yigang.function

import java.sql.{Connection, DriverManager}

import com.yigang.function.FailCntToMySQL.{pas, url, user}
import com.yigang.utils.DBCPUtil
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * Time：2019-12-21 10:49
  * Email： yiie315@163.com
  * Desc：
  *
  * @author： 王一钢
  * @version：1.0.0
  */
object TopNToMySQL extends RichSinkFunction[Tuple2[String,Int]]{

  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    val conn: Connection = DBCPUtil.getConnection
    val sql = "INSERT into topn(pro,cnt) values(?,?) on duplicate key update cnt=?"
    val statement = conn.prepareStatement(sql)
    statement.setString(1,value._1.toString)
    statement.setInt(2,value._2)
    statement.setInt(3,value._2)
    statement.executeUpdate()

    statement.close()
    conn.close()
  }
}
