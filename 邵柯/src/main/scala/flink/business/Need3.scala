package flink.business


import com.alibaba.fastjson.JSON
import flink.common.Constant
import flink.utils.{FlinkUtils, MysqlUtils}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector


/*
@author Yuniko
2019/12/18
*/

/**
  *
  * 充值订单省份 TOP10
  * 以省份为维度统计订单量排名前 10 的省份数据,并且
  * 统计每个省份的订单成功率，只保留一位小数，存入MySQL中，进行前台页面显示。
  *
  *
  */
object Need3 {
  def main(args: Array[String]): Unit = {

    val env = FlinkUtils.getEnv()
    env.setParallelism(1)
    val source = env.readTextFile(Constant.CMCC_URL)
    source.map(line => {
      val jsonObj = JSON.parseObject(line)
      val random = Math.random()
      //val bussinessRst = jsonObj.getString("bussinessRst")
      val bussinessRst = if (random > 0.5) 1 else 0
      val provinceCode = jsonObj.getString("provinceCode")
      ProvincesOrder(provinceCode, bussinessRst)
    }).keyBy(_.provinceCode)
      .process(new MyProvincesOrderProcessFunction).print("Need3")

    env.execute("Need3")
  }

}

case class ProvincesOrder(provinceCode: String, bussinessRst: Int)

class MyProvincesOrderProcessFunction extends KeyedProcessFunction[String, ProvincesOrder, (String, Double)] {

  //定义状态记录 成功以及失败
  private var succeed: ValueState[Double] = _
  private var fairly: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {

    succeed = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("succeed", classOf[Double]))
    fairly = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("fairly", classOf[Double]))

  }

  override def processElement(value: ProvincesOrder, ctx: KeyedProcessFunction[String, ProvincesOrder, (String, Double)]#Context, out: Collector[(String, Double)]): Unit = {

    if (value.bussinessRst == 0) fairly.update(fairly.value() + 1) else succeed.update(succeed.value() + 1)
    val successRate = succeed.value / (succeed.value + fairly.value)
    val stay2 = successRate.formatted("%.2f")

    MysqlUtils.saveRate(value.provinceCode, stay2.toDouble)

    out.collect((value.provinceCode, stay2.toDouble))


  }


}
