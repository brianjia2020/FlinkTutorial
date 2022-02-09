package com.brianjia.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

//    val inputPath = "/Users/chunyangjia/Desktop/self_study/FlinkTutorial/src/main/resources/sensor.txt"
//    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    val inputStream = env.socketTextStream("localhost", 7777)
    //1. convert to sensor reading format
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    // count every sensor's min temp every 15s
    val resultStream = dataStream
      .map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1) // grouped by the first element of the tuple
//      .window(TumblingEventTimeWindows.of(Time.seconds(15))) // tumbling window
//      .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(3))) // sliding window
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
      .timeWindow(Time.seconds(15))
//      .countWindow(1000)
//      .minBy(1)
      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))

    resultStream.print()

    env.execute("min")
  }
}
