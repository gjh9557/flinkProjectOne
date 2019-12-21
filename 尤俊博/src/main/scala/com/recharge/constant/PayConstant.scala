package com.recharge.constant

object PayConstant {
  // flink检查点间隔
  val FLINK_CHECKPOINT_INTERVAL: Long = 5000

  //本地模型下的默认并行度(cpu core)
  val DEF_LOCAL_PARALLELISM = Runtime.getRuntime.availableProcessors

  val REDIS_HOST = "node245"
  val REDIS_PORT = 6379
}
