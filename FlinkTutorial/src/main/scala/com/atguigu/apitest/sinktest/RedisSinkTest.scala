package com.atguigu.apitest.sinktest

import java.util.Properties

import com.atguitu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    //y创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "D:\\zby_workspace\\3.java_workspace\\42.IntelliJ_flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    //转换为样例类类型(简单转换操作)
    val dataStream = inputStream
      .map(data =>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })

    //定义一个FlinkJedisConfigBase
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()


    dataStream.addSink(new RedisSink[SensorReading]( conf, new MyRedisMapper ))

    env.execute("redis sink test")
  }
}

//定义一个RedisMapper
class MyRedisMapper extends RedisMapper[SensorReading]{
  //定义保存数据写入redis的命令，HSET 表名   key value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }

  //将温度值指定为value
  override def getKeyFromData(data: SensorReading): String = data.id

  //将id指定为key
  override def getValueFromData(data: SensorReading): String = data.temperature.toString
}
