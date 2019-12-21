package com.yigang.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Time：2019-12-18 16:56
  * Email： yiigang@126.com
  * Desc：jedis连接池类
  *
  * @author： 王一钢
  * @version：1.0.0
  */
object JedisUtil {

  val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "hadoop01",6379)

  def getJedisClient(): Jedis = {
    jedisPool.getResource
  }

  def releaseJedis(jedis: Jedis): Unit ={
    if(jedis != null){
      jedisPool.returnResource(jedis)
    }
  }
}
