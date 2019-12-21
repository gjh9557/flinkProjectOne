package com.qf.flink_24.day01

import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.immutable.TreeMap

object TopN {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //word,word,word
    import org.apache.flink.api.scala._
    //word,word,word
    val lineds: DataStream[String] = env.socketTextStream("node1",9999)
    val wordds: DataStream[String] = lineds.flatMap(str => {
      val arr = str.split(",")
      arr
    })
    val tupds: DataStream[(String, Int)] = wordds.map((_,1))
    //定义滑动窗口，聚合一个时间间隔内的数据
    val windowds: DataStream[(String, Int)] = tupds.keyBy(0)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(20)))
      .sum(1)
    //全局topN
    val resultds: DataStream[(String, Int)] = windowds.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(20)))
      .process(new TopnFunction(10))
    //SINK
    resultds.print().setParallelism(1)
    env.execute("flinkwc")
  }



  private class TopnFunction(topSize: Int) extends ProcessAllWindowFunction[(String,Int),(String,Int),TimeWindow] { // TODO Auto-generated constructor stub


    def process(context: Context, input: Iterable[(String,Int)],  out: Collector[(String,Int)]): Unit = {
      //treemap按照key降序排列，相同count值不会覆盖
      var treemap = new TreeMap[Int,(String,Int)]()
      (new Ordering[Int]() {
      override def compare(y: Int, x: Int): Int = if (x < y) -1
      else 1
      })

      import scala.collection.JavaConversions._
      for (element <- input) {
        treemap = treemap ++ List((element._2,element))
        if (treemap.size > topSize) { //保留前面TopN个元素
          treemap.dropRight(1)
        }
      }
      for (entry <- treemap.entrySet) {
        out.collect(entry.getValue)
      }
    }
  }

}