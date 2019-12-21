package flink.utils

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
@author Yuniko
2019/12/17
*/

object FlinkUtils {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def getEnv(): StreamExecutionEnvironment = {

    env

  }

}
