package com.atguigu.apitest.sinktest

import java.util.Properties

import com.atguitu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    //0.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val inputPath = "D:\\zby_workspace\\3.java_workspace\\42.IntelliJ_flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
//    val inputStream = env.readTextFile(inputPath)

    //从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","consumer-group")
    val stream = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))


    //1.先转换为样例类类型(简单转换操作)
    val dataStream = stream
      .map(data =>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble).toString
      })

    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092","sinktest",new SimpleStringSchema()))

    env.execute("kafka sink test")
  }

}
