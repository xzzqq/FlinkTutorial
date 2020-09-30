package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

//流处理Word count
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(8)
    //env.disableOperatorChaining()

    val paramTool:ParameterTool = ParameterTool.fromArgs(args)
    val host:String =paramTool.get("host")
    val port:Int    = paramTool.getInt("port")


    // 接收一个socket文本流
    //val inputDataStream: DataStream[String] = env.socketTextStream("localhost",7777)
    val inputDataStream: DataStream[String] = env.socketTextStream(host,port)
    // 进行转换处理统计
    val resultDataStream: DataStream[(String,Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)     //.disableChaining()
      .map((_,1))             //.startNewChain()
      .keyBy(0)
      .sum(1)

    resultDataStream.print().setParallelism(1)

    //启动任务执行
    env.execute("stream word count")
  }
}
