package com.brianjia.apitest

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "/Users/chunyangjia/Desktop/self_study/FlinkTutorial/src/main/resources/sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    val aggStream = dataStream
      .keyBy(_.id)
      .minBy("temperature")

    aggStream.print()

    env.execute("transform")
  }
}
