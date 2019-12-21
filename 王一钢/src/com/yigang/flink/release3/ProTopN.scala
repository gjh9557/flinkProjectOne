package com.yigang.flink.release3

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.yigang.function.{ProRateToMySQL, TopNToMySQL}
import com.yigang.test.Source_Demo
import com.yigang.utils.DateUtil
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.immutable.TreeMap

/**
  * Time：2019-12-20 8:58
  * Email： yiie315@163.com
  * Desc：需求三：
  *               以省份为维度统计订单量排名前 10 的省份数据,
  *               并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中
  *
  * @author： 王一钢
  * @version：1.0.0
  */
object ProTopN {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //从kafka中实时拉取数据
    val pro = new Properties()
    pro.load(Source_Demo.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    val consumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("pay1",new SimpleStringSchema(),pro)
    consumer.setStartFromEarliest()
    val data: DataStream[String] = env.addSource(consumer)

    //读取省份映射表
    val city: DataStream[String] = env.readTextFile("data/flink/city.txt")

    //取出json中用到的字段并做简单处理
    val base_data: DataStream[(Int, Int,Int)] = data
      .filter(new FilterFunction[String] {
        override def filter(t: String): Boolean = {
          //过滤出只是调用充值服务接口的数据
          return JSON.parseObject(t).getString("serviceName").equals("reChargeNotifyReq")
        }
      }).map(line => {
      val bussinessRst: String = JSON.parseObject(line).getString("bussinessRst")
      val receiveNotifyTime: String = JSON.parseObject(line).getString("receiveNotifyTime")
      val provinceCode: Int = JSON.parseObject(line).getInteger("provinceCode")
      //充值成功标志 成功为 1  失败为 0
      val tag = if (bussinessRst.equals("0000") && receiveNotifyTime != null && !"".equals(receiveNotifyTime)) 1 else 0
      //省份代码 结果代码）
      (provinceCode, tag,1)
    })

    //统计每个省份订单量
    val sum1: DataStream[(Int, Int)] = base_data.map(x => (x._1, 1))
      .keyBy(0)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .sum(1)

    //求出订单量前10的省份代码
    val proCode_cnt: DataStream[(Int, Int)] = sum1.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new TopNFunction(10))

    /**
      * 订单量前10的省份
      */
    val res1: DataStream[(String, Int)] = city.map(x => {
      val line = x.split("\\s")
      val num = line(0).toInt
      val pro = line(1)
      (num, pro)
    }).join(proCode_cnt) //进行join
      .where(x => x._1)
      .equalTo(x => x._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(5)))
      .apply((x, y) => (x._2, y._2)) //省份  订单量


    //求出每个省份代码的成功率
    val proCode_rate: DataStream[(Int, Int, Int, String)] = base_data
      .map(x => (x._1, x._2, x._3, ""))
      .keyBy(0)
      //省份代码 成功数 订单总数 成功率
      .reduce((x, y) => (x._1, x._2 + y._2, x._3 + y._3, ((x._2 + y._2) * 1.0 / (x._3 + y._3) * 100).formatted("%.2f") + "%"))

    /**
      * 每个省份的成功率
      */
    val res2: DataStream[(String, String)] = city.map(x => {
      val line = x.split("\\s")
      val num = line(0).toInt
      val pro = line(1)
      (num, pro)
    }).join(proCode_rate) //进行join
      .where(x => x._1)
      .equalTo(x => x._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(5)))
      .apply((x, y) => (x._2, y._4)) //省份  成功率
        .filter(x=>(!x._2.isEmpty))

    //topN存入数据库
   // res1.addSink(TopNToMySQL)
    //每个省份成功率存入数据库
    res2.addSink(ProRateToMySQL)

    env.execute()
  }

  class TopNFunction(topSize: Int) extends ProcessAllWindowFunction[(Int,Int),(Int,Int),TimeWindow]{
    override def process(context: Context, input: Iterable[(Int, Int)], out: Collector[(Int, Int)]): Unit = {
      //treemap按照key降序排列，相同count值不会覆盖
      var treemap: TreeMap[Int, (Int, Int)] = new TreeMap[Int, (Int, Int)]()(
        new Ordering[Int]() {
          override def compare(x: Int, y: Int): Int = {
            if (x < y) -1
            else 1
          }
        }
      )

      import scala.collection.JavaConversions._
      for(ele <- input){
        treemap = treemap ++ List((ele._2,ele))
        //保留前面TopN个元素
        if(treemap.size>topSize){
          treemap.dropRight(1)
        }
      }
      for(entry <- treemap.entrySet){
        out.collect(entry.getValue)
      }
    }
  }
}
