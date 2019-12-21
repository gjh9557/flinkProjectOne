package flink.common

import java.util.Properties

/*
@author Yuniko
2019/12/17
*/


object Constant {
  lazy val ORDER_LOG ="D:\\IdeaIU_work_space1\\TopUpPlatform\\src\\main\\resources\\order.log"
  lazy val LOGIN_URL ="D:\\IdeaIU_work_space1\\TopUpPlatform\\src\\main\\resources\\logEvent.log"
  lazy val CITY_URL = "D:\\IdeaIU_work_space1\\TopUpPlatform\\src\\main\\resources\\city.txt"
  lazy val REDIS_HOST = "hadoop3"
  lazy val REDIS_PORT = 6379
  lazy val CMCC_URL = "D:\\IdeaIU_work_space1\\TopUpPlatform\\src\\main\\resources\\cmcc.json"
  lazy val USER_BEHAVIOR_URL = "D:\\IdeaIU_work_space1\\TopUpPlatform\\src\\main\\resources\\UserBehavior.csv"
  lazy val USER_BEHAVIOR_FIELD_ARR = Array[String]("userId", "itemId", "categoryId", "behavior", "timestamp")
  lazy val HOT_TOPIC ="hot"
  lazy val PAY_TOPIC ="pay"
  lazy val APACHE_LOG ="D:\\IdeaIU_work_space1\\TopUpPlatform\\src\\main\\resources\\apachetest.log"

   //class UserBehavior(val userId: Long, val itemId: Long, val categoryId: Int, val behavior: String, val timestamp: Long)


  /**
    * 窗口输出类型
    *
    * @param itemId    商品id
    * @param WindowEnd 窗口的结束时间
    * @param viewCount 商品的点击量
    */
  case class ItemViewCount(itemId: Long, WindowEnd: Long, viewCount: Long)

  /**
    * Kafka 相关配置
    *
    * @return
    */
  lazy val BROKER_LIST = "hadoop1:9092,hadoop2:9092,hadoop3:9092"
  lazy val ZOOKEEPER_LIST = "hadoop1:2181,hadoop2:2181,hadoop3:2181"
  lazy val BROKER_LIST_BAK = "node245:9092"
  lazy val ZOOKEEPER_LIST_BAK = "node245:2181"
  lazy val BOOTSTRAP_SERVERS_BAK ="node245:9092"

  lazy val properties = new Properties()
  //properties.setProperty("metadata.broker.list", BROKER_LIST_BAK);
 // properties.setProperty("zookeeper.connect", ZOOKEEPER_LIST_BAK);
  properties.setProperty("bootstrap.servers",BOOTSTRAP_SERVERS_BAK)

  properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")


  //是否反馈消息 0是不反馈消息 1是反馈消息
 // properties.setProperty("request.required.acks","1");

  def getProperties(): Properties ={
   properties

  }

}


/**
  *
  *
  * Look 当使用InputFormat + 样例类的时候报异常 => 不能写样例类 会报异常  使用普通类 并给无参构造函数
  * 用户行为
  *
  * @param userId     用户id
  * @param itemId     商品id
  * @param categoryId 商品类别id
  * @param behavior   用户行为  =>  pv | buy | cart | fav
  * @param timestamp  行为发生的时间戳
  *
  *
  */


class UserBehavior(var userId: Long, var itemId: Long,var categoryId:Long,var behavior:String,var timestamp:Long) {
  def this() {
    this(0L,0L,0L,null,0L )
  }


  override def toString = s"UserBehavior($userId, $itemId, $categoryId, $behavior, $timestamp)"
}
