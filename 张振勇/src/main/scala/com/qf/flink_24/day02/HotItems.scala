package com.qf.flink_24.day02

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
  * Description：
  * Copyright (c) ，2019 ， zzy 
  * This program is protected by copyright laws. 
  * Date： 2019年12月19日 
  *
  * @author 张振勇
  * @version : 0.1
  */
//定义输入数据的样例类
case class UserBehavior( userID: Long, itemId: Long, categoryId: Long, behavior: String, timeStamp: Long)
//定义窗口聚合结果样例类
case class ItemViewCount(  itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //source
    val datastream = env.readTextFile("D:\\ideaWork\\Flink_24\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toLong,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timeStamp * 1000L)
    //transform
    val processStream = datastream
        .filter(_.behavior.equals("pv"))
        .keyBy(_.itemId)
        .timeWindow(Time.hours(1),Time.minutes(5))
        .aggregate( new CountAgg(), new WindowResult())
        .keyBy(_.windowEnd)
        .process( new TopNHotItems(3) )

    //sink
    processStream.print()

    env.execute()
  }
}

//自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义预聚合函数 计算平均数
class AvgAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double]{
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timeStamp, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
}
//自定义窗口函数
class WindowResult() extends WindowFunction[Long, ItemViewCount,Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//自定义的处理函数
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState( new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //把每条数据存入状态列表
    itemState.add(i)
    //注册一个定时器
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  //定时器触发时,对所有的数据排序,并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //将所有state的数据取出,放到一个ListBUffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for(item <- itemState.get()){
      allItems += (item)
    }

    //按照count大小排序,并取前n个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //清空状态
    itemState.clear()

    //将排名结果格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("时间: ").append(new Timestamp( timestamp - 1)).append("\n")
    //输出每一个商品的信息
    for(i <- sortedItems.indices ){
      val currentItem = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("===================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

