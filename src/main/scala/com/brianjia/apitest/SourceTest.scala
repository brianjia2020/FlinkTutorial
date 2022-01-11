package com.brianjia.apitest

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties
import scala.util.Random

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

    val stream1 = env.fromCollection(dataList)

    //2. read from file
    val inputPath = "src/main/resources/sensor.txt"
    val stream2 = env.readTextFile(inputPath)

    //3. read from kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("auto.offset.reset", "latest")
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    //4. self-defined source
    val stream4 = env.addSource(new MySourceFunction())
    stream4.print()

    env.execute("source test")
  }

  class MySourceFunction() extends SourceFunction[SensorReading] {
    // define a flag to represent the source has stably send out data
    var running: Boolean = true

    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
      //define a random number generator
      val rand = new Random()
      var curTemp = List.range(1, 10).map(i => ("sensor_" + i, rand.nextDouble() * 100))

      //need a infinite loop to continuously generate data
      while(running) {
        //minor adjust on initial data
        curTemp = curTemp.map(
          data => (data._1, data._2 + rand.nextGaussian())
        )

        val curTime = System.currentTimeMillis()
        curTemp.foreach(
          //collect to send out the data
          data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
        )

        //wait for some time
        Thread.sleep(100)
      }
    }

    override def cancel(): Unit = {
      running = false;
    }
  }
}
