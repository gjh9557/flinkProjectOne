import redis.clients.jedis.Jedis

/*
@author Yuniko
2019/12/21
*/

object  Test3 {
  def main(args: Array[String]): Unit = {
    val a = 1L
    val jedis = new Jedis("node245", 6379)
    jedis.setbit("yuniko:"+a.toString,0,true)
    jedis.setbit("yuniko:"+a.toString,1,true)
    jedis.setbit("yuniko:"+a.toString,2,true)
    jedis.setbit("yuniko:"+a.toString,3,true)
    jedis.setbit("yuniko:"+a.toString,4,true)


  }
}
