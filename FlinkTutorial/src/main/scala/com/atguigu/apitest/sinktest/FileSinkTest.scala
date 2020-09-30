package com.atguigu.apitest.sinktest

import com.atguitu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object FileSinkTest {
  def main(args: Array[String]): Unit = {
    //0.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "D:\\zby_workspace\\3.java_workspace\\42.IntelliJ_flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    //1.先转换为样例类类型(简单转换操作)
    val dataStream = inputStream
      .map(data =>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })

    //dataStream.print()
    //1、直接通过writeAsCsv导入文本文件中-已被弃用
    //dataStream.writeAsCsv("D:\\zby_workspace\\3.java_workspace\\42.IntelliJ_flink\\FlinkTutorial\\src\\main\\resources\\out.txt")

    //2、通过addSink，写入文件中
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("D:\\zby_workspace\\3.java_workspace\\42.IntelliJ_flink\\FlinkTutorial\\src\\main\\resources\\out1.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    env.execute("sink test")
  }

}
