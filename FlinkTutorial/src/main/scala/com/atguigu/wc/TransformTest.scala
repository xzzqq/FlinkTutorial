package com.atguigu.wc

import com.atguitu.apitest.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TransformTest {
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
      .filter(new MyFilter)
      //.filter(_.contains("flink"))

    //2.分组聚合，输出每个传感器当前最小值
    val aggStream = dataStream
      .keyBy("id")
      .minBy("temperature")
    //aggStream.print()

    //3.需要输出当前最小的温度值，以及最近的时间戳,要用reduce
    val resultStream = dataStream
        .keyBy("id")
        //.reduce((curState,newData) =>
        //   SensorReading(curState.id,newData.timestamp,curState.temperature.min(newData.temperature)
        //   )
        .reduce(new MyReduceFunction)


    //4.多流转换操作
    //4.1 分流，将传感器温度数据分成低温，高温两条流
    val splitStream = dataStream
      .split(data => {
        if(data.temperature > 30.0) Seq("high") else Seq("low")
      })
    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high","low")
    //highTempStream.print("high")
    //lowTempStream.print("low")
    //allTempStream.print("all")

    // 4.2 合流，connect
    val warningStream = highTempStream.map(data => (data.id,data.temperature))
    val connectedStream = warningStream.connect(lowTempStream)

    //用coMap对数据进行分别处理
    val coMapResultStream = connectedStream
      .map(
        warningData => (warningData._1, warningData._2, "warning"),
        lowTempData => (lowTempData.id, "healthy")
      )
    //coMapResultStream.print("coMap")

    //4.3 union 合流(两个流的类型必须一致)
    val unionStream = highTempStream.union(lowTempStream)


    env.execute("transform test")

  }
}

class MyReduceFunction extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id,value2.timestamp,value1.temperature.min(value2.temperature))
}

class MyFilter extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean =
    value.id.startsWith("sensor_1")

}

class MyMapper extends MapFunction[SensorReading,String]{
  override def map(value: SensorReading): String = value.id + " temperature"
}

//富含数，可以获取到运行时上下文，还有一些生命周期方法
class MyRichMapper extends RichMapFunction[SensorReading,String]{
  override def open(parameters: Configuration): Unit = {
    
  }

  override def map(value: SensorReading): String = value.id + " temperature"

}
