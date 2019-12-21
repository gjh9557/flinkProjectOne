package flink.joincity

import com.alibaba.fastjson.JSON
import flink.business.ProvincesOrder
import flink.common.Constant
import flink.utils.FlinkUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


/*
@author Yuniko
2019/12/18
*/
/**
  * 实现 城市id与 城市名的 join
  *
  *
  */
object CityJoin {

  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getEnv()
    env.setParallelism(1)
    val CmccSource = env.readTextFile(Constant.CMCC_URL)
    val ProvincesOrderDataStream = CmccSource.map(line => {
      val jsonObj = JSON.parseObject(line)
      val random = Math.random()
      //val bussinessRst = jsonObj.getString("bussinessRst")
      val bussinessRst = if (random > 0.5) 1 else 0
      val provinceCode = jsonObj.getString("provinceCode")
      ProvincesOrder(provinceCode, bussinessRst)
    })
    val citySource = env.readTextFile(Constant.CITY_URL)
    val cityDataStream = citySource.map(line => Provinces(line.split(" ")(0),line.split(" ")(1)))
    ProvincesOrderDataStream
      .join(cityDataStream)
      .where(t=>t.provinceCode)
      .equalTo(t=>t.id)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .apply((provincesOrder,provinces)=>{

        (provincesOrder.provinceCode,provinces.name,provincesOrder.bussinessRst)

      }).print("内容")
     env.execute("City")

  }

}
case class Provinces(id:String,name:String)
