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

    // 2. stream based on the key
    val aggStream = dataStream
      .keyBy(_.id)
      .minBy("temperature")

    // 3. reduce by current timestamp and get the current min temperature
    val resultStream = dataStream.keyBy(_.id).reduce(
      (curState, newData) =>
        SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature))
    )

    // 4. multiple stream
    // 4.1 split stream (split the sensor reading into high/low streams)
    val highStream = dataStream.filter(
      data => data.temperature >= 30
    )

    val lowStream = dataStream.filter(
      data => data.temperature < 30
    )

//    highStream.print("high")
//    lowStream.print("low")

    //4.2 connect
    val warningStream = highStream.map(
      data => (data.id, data.temperature)
    )
    val connectedStreams = warningStream
      .connect(lowStream)
    val coMapStream = connectedStreams
      .map(
        warningData => (warningData._1, warningData._2, "warning"),
        lowTempData => (lowTempData.id, "healthy")
      )

//    coMapStream.print("coMap")

    //4.3 union
    val unionStream = highStream.union(lowStream)

    env.execute("transform")
  }
}
