/*
@author Yuniko
2019/12/18
*/ object Test2 {
  def main(args: Array[String]): Unit = {
    val line = "83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png"
    val regex = "(.*) - - (.*)+0000(.*)(.*)$".r
    // ip:String,userId:String,eventTime:Long,method:String,url:String

    val (ip: String, eventTime: String, method: String, url: String) = line match {
      case regex(x, y, z, d) => (x, y, z, d)
    }
    println(ip + " " + method)
  }
}
