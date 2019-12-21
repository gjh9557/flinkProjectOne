package flink.joincity

import com.alibaba.fastjson.JSON
import flink.business.ProvincesOrder
import flink.common.Constant
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

import scala.collection.mutable
import scala.io.Source
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.HeapBroadcastState
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/*
@author Yuniko
2019/12/19
*/
/**
  * 数据量不大 使用广播变量
  *  Look 还有问题 还需要 操作
  */


object broadcastConnect {
  def main(args: Array[String]): Unit = {
    //读取文件中的数据放入 map
    val hashMap = new mutable.HashMap[String, String]()
    val source = Source.fromFile(Constant.CITY_URL)
    source.getLines().toList.foreach(
      line => {
        val argsArr = line.split(" ")
        val id = argsArr(0)
        val name = argsArr(1)
        hashMap += id -> name
      }
    )
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val broadcastSource = env.fromCollection(hashMap.toList)
    //broadcastSource.print("")

    //TODO
    val CITY_DESCRIPTOR =
      new MapStateDescriptor[String, String]("cityMapping", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    /**
      * Look 如果存在 不是基础类类型
      *
      *    TypeInformation.of(new TypeHint<Rule>(){}
      *
      * new MapStateDescriptor[String, Rule]("cityMapping", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Rule>(){} )
      */

    //并行度设置为1 减少发送
    val citybroadcast = broadcastSource.setParallelism(1).broadcast(CITY_DESCRIPTOR)


    val CmccSource = env.readTextFile(Constant.CMCC_URL)


    val ProvincesOrderDataStream = CmccSource.map(line => {
      val jsonObj = JSON.parseObject(line)
      val random = Math.random()
      //val bussinessRst = jsonObj.getString("bussinessRst")
      val bussinessRst = if (random > 0.5) 1 else 0
      val provinceCode = jsonObj.getString("provinceCode")
      ProvincesOrder(provinceCode, bussinessRst)
    })

    ProvincesOrderDataStream.connect(citybroadcast).process(


      new BroadcastProcessFunction[ProvincesOrder,(String,String),(String,String,Int)]{
         override def processElement(value: ProvincesOrder, ctx: BroadcastProcessFunction[ProvincesOrder, (String, String), (String, String, Int)]#ReadOnlyContext, out: Collector[(String, String, Int)]): Unit = {

           val readOnleyBroadcast = ctx.getBroadcastState(CITY_DESCRIPTOR).asInstanceOf[HeapBroadcastState[String,String]]

           val name = readOnleyBroadcast.get(value.provinceCode)



           val iterator = readOnleyBroadcast.immutableEntries().iterator()
           while (iterator.hasNext()){
             val entry =iterator.next();
            println(entry.getKey+" "+entry.getValue)
           }
           out.collect((value.provinceCode,name,value.bussinessRst))
         }

         override def processBroadcastElement(value: (String, String), ctx: BroadcastProcessFunction[ProvincesOrder, (String, String), (String, String, Int)]#Context, out: Collector[(String, String, Int)]): Unit = {
           //收到广播流
           val readOnleyBroadcast = ctx.getBroadcastState(CITY_DESCRIPTOR)

           val iterator = readOnleyBroadcast.immutableEntries().iterator()
           while (iterator.hasNext()){
             val entry =iterator.next();
             println(entry.getKey+" "+entry.getValue)
           }


         }
       }
    ).print("cityBroadcast")
    env.execute(broadcastConnect.getClass.getName)


  }
}
