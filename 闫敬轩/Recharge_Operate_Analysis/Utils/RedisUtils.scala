package Recharge_Operate_Analysis.Utils

import Recharge_Operate_Analysis.Constant.Constant
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}


/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/9 16:08
  * @version 1.0
  */
object RedisUtils {
  private val redisHost: String = Constant.JEDIS_HOST
  private val redisPort: Int = Constant.JEDIS_PORT
  private val maxTotal = Constant.JEDIS_MAX_TOTAL
  private val maxIdle = Constant.JEDIS_MAX_IDLE
  private val redisTimeOut: Int = Constant.JEDIS_TIME_OUT // ms
  private val minIdle = Constant.JEDIS_MIN_IDLE
  private val onborrow = Constant.JEDIS_ON_BORROW
  @transient private var pool: JedisPool = _
  makePool(redisHost, redisPort, redisTimeOut, maxTotal, maxIdle, minIdle,onborrow)

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int,onborrow:Boolean): Unit = {
    makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, onborrow, false, 10000)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
               testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
    if (pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

      val hook = new Thread {
        override def run: Unit = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool:Jedis= {
    assert(pool != null)
    pool.getResource
  }

  def readRedis(key:String) = {
    val jedis = this.getPool
    val businame = jedis.hget(Constant.JEDIS_RECHARGE_OVERVIEW,key)
    jedis.close()
    businame
  }

  def writeRedis(key:String,value:String) = {
    val jedis = this.getPool
    jedis.hset(Constant.JEDIS_RECHARGE_OVERVIEW,key,value)
    jedis.close()
  }


//  def main(args: Array[String]) = {
//    val jedis = getPool
//    val value = jedis.set("name", "test")
//    println(s"value is $value")
//    jedis.close()
//  }
}
