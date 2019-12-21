package flink.utils

import flink.business.Total_Bak
import flink.common.Constant
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


/*
@author Yuniko
2019/12/9
*/

/**
  * Redis 工具类
  *
  */
object RedisUtils {
  private var pool: JedisPool = null

  def getJedisFromPool(): Jedis = {

    if (pool == null) {
      val config: JedisPoolConfig = new JedisPoolConfig
      config.setMaxIdle(30)
      config.setMaxTotal(100)
      config.setTestOnBorrow(true)
      pool = new JedisPool(config, Constant.REDIS_HOST, Constant.REDIS_PORT)

    }
    pool.getResource
  }

  def saveNetworkInfo(total: Total_Bak): Unit = {

    val redis = getJedisFromPool()
    val provinceCode = total.provinceCode
    val timeTotal = total.TimeTotal
    val successTotal = total.successTotal
    val orderSize = total.orderSize
    val moneyTotal = total.moneyTotal


    redis.hset(s"pay:${provinceCode}", "orderSize", s"${orderSize}")
    redis.hset(s"pay:${provinceCode}", "successTotal", s"${successTotal}")
    redis.hset(s"pay:${provinceCode}", "moneyTotal", s"${moneyTotal}")
    redis.hset(s"pay:${provinceCode}", "timeTotal", s"${timeTotal}")

    redis.close()

  }


}
