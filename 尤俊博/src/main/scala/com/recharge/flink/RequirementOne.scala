package com.recharge.flink

import java.util.Calendar

import com.recharge.constant.PayConstant
import com.recharge.model.PayData
import com.recharge.util.{FlinkUtil, PayJasonReader}
import org.apache.flink.api.common.functions.{AggregateFunction, FilterFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

/**
  *  统计全网的订单量、充值金额、充值成功数
  *  实时统计，每小时一次，数据写入redis
  */
object RequirementOne {
  def main(args: Array[String]): Unit = {
    //1. 获取流式执行环境和必要参数
    val env: StreamExecutionEnvironment = FlinkUtil.getEnviornment()
    val REDIS_HOST = PayConstant.REDIS_HOST
    val REDIS_PORT = PayConstant.REDIS_PORT
    //2. 获取数据通过自定义数据源
    val payDataStream: DataStream[(Long, Double, Long)] = env.addSource(new PayJasonReader("dir/in/cmcc.json"))
      .filter(new PayFilter())
      .keyBy(payData => payData.getServiceName.trim)
      .timeWindow(Time.hours(1))
      //      .aggregate(new PayAggregate())
      .process(new PayProcess())
    //3. 配置Redis环境
    val cnf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost(REDIS_HOST).setPort(REDIS_PORT).build()
    payDataStream.addSink(new RedisSink[(Long, Double, Long)](cnf, new RedisPayMapper()))

    env.execute
  }
}

class PayFilter() extends FilterFunction[PayData] {
  override def filter(t: PayData): Boolean = {
    if (t.getBussinessRst != "0000") {
      return false
    } else {
      return true
    }
  }
}


/**
  * (Long, Double, Long)
  * 充值订单量 充值金额  充值成功数
  */
class PayAggregate() extends AggregateFunction[PayData, (Long, Double, Long), (Long, Double, Long)] {
  //1. 初始化累加变量
  override def createAccumulator(): (Long, Double, Long) = (0L, 0.0, 0L)

  //2. 累加逻辑
  override def add(in: PayData, acc: (Long, Double, Long)): (Long, Double, Long) = {
    if (in.getBussinessRst != "0000") {
      (acc._1 + 1, acc._2, acc._3)
    } else {
      (acc._1 + 1, acc._2 + in.getChargefee, acc._3 + 1)
    }
  }

  //3. 活得最终结果
  override def getResult(acc: (Long, Double, Long)): (Long, Double, Long) = acc

  //4. 分区间聚合
  override def merge(acc: (Long, Double, Long), acc1: (Long, Double, Long)): (Long, Double, Long) = {
    (acc._1 + acc1._1, acc._2 + acc1._2, acc._3 + acc1._3)
  }
}


class PayProcess() extends ProcessWindowFunction[PayData, (Long, Double, Long), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[PayData], out: Collector[(Long, Double, Long)]): Unit = {
    var reslut: (Long, Double, Long) = (0L, 0.0, 0L)
    for (payData <- elements) {
      if (payData.getBussinessRst == "0000") {
        reslut = (reslut._1 + 1, reslut._2 + payData.getChargefee, reslut._3 + 1)
      } else {
        reslut = (reslut._1 + 1, reslut._2, reslut._3)
      }
    }
    // 这里还可以算一些每个小时的成功率
    out.collect(reslut)
  }
}

class RedisPayMapper() extends RedisMapper[(Long, Double, Long)] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "pay_data")
  }

  override def getKeyFromData(t: (Long, Double, Long)): String = {
    // 因为以每个小时开滚动窗口，就以每个小时的聚合结果写入redis
    Calendar.getInstance().get(Calendar.HOUR).toString
  }

  override def getValueFromData(t: (Long, Double, Long)): String = {
    t.toString()
  }
}

