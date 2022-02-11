package com.brianjia.apitest

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500)

//    val inputPath = "/Users/chunyangjia/Desktop/self_study/FlinkTutorial/src/main/resources/sensor.txt"
//    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    val inputStream = env.socketTextStream("localhost", 7777)
    //1. convert to sensor reading format
    val dataStream = inputStream
      .map(
        data => {
          val arr = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )
//      .assignAscendingTimestamps(_.timestamp * 1000L)
      // delay as 3s and time as the input event time
      // 3s is a guess, you need to adjust based on different realistic scenarios
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L;
      })

    val lateTag = new OutputTag[(String, Double, Long)]("late")

//      .assignTimestampsAndWatermarks(WatermarkStrategy[SensorReading])

    // count every sensor's min temp every 15s
    val resultStream = dataStream
      .map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1) // grouped by the first element of the tuple
//      .window(TumblingEventTimeWindows.of(Time.seconds(15))) // tumbling window
//      .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(3))) // sliding window
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.seconds(60))
      .sideOutputLateData(lateTag)
//      .countWindow(1000)
//      .minBy(1)
      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))

    resultStream.getSideOutput(lateTag).print("late")
    resultStream.print("result")

    env.execute("min")
  }
}
