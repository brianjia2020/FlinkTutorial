package com.brianjia.apitest

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

//define a class mimicking an IOT device for temperature
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    //create env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1. read from a collection
    val dataList = List(
      SensorReading("sensor1", 1, 35.8),
      SensorReading("sensor1", 2, 36.2),
      SensorReading("sensor1", 3, 37.5),
      SensorReading("sensor1", 4, 38.2),
      SensorReading("sensor1", 5, 39.1)
    )

    val stream1 = env.fromCollection(dataList);

    //2. read from file
    val inputPath = "src/main/resources/sensor.txt"
    val stream2 = env.readTextFile(inputPath)

    //3. read from kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("auto.offset.reset", "latest")
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(),properties ))
      stream2.print()

    env.execute("source test");
  }
}
