package com.yigang.utils

import java.text.SimpleDateFormat

/**
  * Time：2019-12-18 17:08
  * Email： yiigang@126.com
  * Desc：
  *
  * @author： 王一钢
  * @version：1.0.0
  */
object DateUtil {

  /**
    * 获取充值时间
    * @param start
    * @param endTime
    * @return
    */
  def caculateRqt(start: String, endTime: String): Long = {
    if(start==null || "".equals(start) || endTime==null || "".equals(endTime)){
      return 0
    }
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")

    val st = dateFormat.parse(start.substring(0, 17)).getTime
    val et = dateFormat.parse(endTime).getTime

    et - st
  }
}
