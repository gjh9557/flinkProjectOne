package com.recharge.util

import com.recharge.constant.PayConstant
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.{Logger, LoggerFactory}

object FlinkUtil {
  val logger: Logger = LoggerFactory.getLogger("FlinkUtil")

  def getEnviornment(): StreamExecutionEnvironment = {
    var env: StreamExecutionEnvironment = null
    try {
      env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(PayConstant.DEF_LOCAL_PARALLELISM)
      env.enableCheckpointing(PayConstant.FLINK_CHECKPOINT_INTERVAL)
      //      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(500, TimeUnit.MILLISECONDS)))
      //      env.setStateBackend(new FsStateBackend("hdfs://hadoop100:9000/ck", false))
    }catch {
      case ex:Exception=>{
        println(s"FlinkHelper create flink context occur exception：FlinkUtil.getEnviornment, msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }
    env
  }
}
