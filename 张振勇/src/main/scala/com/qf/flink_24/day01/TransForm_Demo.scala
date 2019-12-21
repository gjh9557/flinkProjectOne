package com.qf.flink_24.day01

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment}

object TransForm_Demo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //word,word,word
    import org.apache.flink.api.scala._
    val lineds: DataStream[String] = env.socketTextStream("node1",8989)
    val wordds: DataStream[String] = lineds.flatMap(str => {
      val arr = str.split(",")
      arr
    })
    val tupds: DataStream[(String, Int)] = wordds.map((_,1))
    val tupds1 = wordds.map(new MapFunction[String,(String,Int)] {
      override def map(t: String): (String,Int) = {
        (t,1)
      }
    })
    val wordCountds: DataStream[wordCount] = wordds.map(wordCount(_,1))
    val keyds0: KeyedStream[wordCount, Tuple] = wordCountds.keyBy("word")
    val keyds: KeyedStream[(String, Int), Tuple] = tupds.keyBy(0)
    val reducedDS: DataStream[(String, Int)] = keyds.reduce((a:(String,Int),b:(String,Int))=>(a._1,a._2+b._2))
    //count
    keyds.max("count")
    keyds.max(1)
    //word,count
    keyds.maxBy("count")
    keyds.maxBy(1)

    //union,connect
    //union合并多道流，要求每道流的数据类型一致
    val ds2: DataStream[String] = env.fromCollection(Seq("scala", "java", "python"))
    val unionds: DataStream[String] = ds2.union(wordds)
   // ds2.union(wordCountds)
    //connect连接两道流，每道流的数据类型可以不一致
    val connectds: ConnectedStreams[String, wordCount] = ds2.connect(wordCountds)
    val mapds1:DataStream[wordCount]= connectds.map(new CoMapFunction[String, wordCount,wordCount] {
      override def map1(in1: String): wordCount = {
        wordCount(in1, 1)
      }
      override def map2(in2: wordCount): wordCount = {
        in2
      }
    })
    connectds.map(str=>wordCount(str,1),wc=>wc)
    //split,select
    val numds = env.fromCollection(Seq(1,2,3,4,5,6,7,8,9))
    //根据奇偶性做个split
    val splitds: SplitStream[Int] = numds.split(num=>if(num%2==0) Seq("even") else Seq("odd"))
    val selectds: DataStream[Int] = splitds.select("even")
    //SINK
    reducedDS.print()
    env.execute("flinkwc")
  }

}
case class wordCount(word:String,count:Int)