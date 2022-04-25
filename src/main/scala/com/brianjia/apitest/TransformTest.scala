package com.brianjia.apitest

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class TransformTest {
  def main(args: Array[String]): Unit = {
    //1. start the env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2. read in the file
    val sensorReading = env.readTextFile("src/main/resources/sensor.txt")

    //3. tranform
    val dataStream = sensorReading.map(
      data => {
        val arr = data.split(",")
        val arr2 = arr.map(word => word.trim())
        SensorReading(arr2(0),arr2(1).toLong, arr2(2).toDouble)
      }
    )

    // output the minimum value of each sensor
//    dataStream.keyBy()

  }
}
