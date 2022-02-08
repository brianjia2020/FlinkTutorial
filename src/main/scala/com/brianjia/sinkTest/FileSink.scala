package com.brianjia.sinkTest

import com.brianjia.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "/Users/chunyangjia/Desktop/self_study/FlinkTutorial/src/main/resources/sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    //1. convert to sensor reading format
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    //2. sink to file
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("src/main/resources/out"),
        new SimpleStringEncoder[SensorReading]("UTF-8")
      ).build()
    )

    env.execute("fileSinkTest")

  }
}
