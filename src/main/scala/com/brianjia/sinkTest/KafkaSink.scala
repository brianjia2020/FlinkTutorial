package com.brianjia.sinkTest

import com.brianjia.apitest.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._

import java.util.Properties

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val inputPath = "/Users/chunyangjia/Desktop/self_study/FlinkTutorial/src/main/resources/sensor.txt"
//    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    // 1.read from kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("auto.offset.reset", "latest")
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))


    //2. convert to sensor reading format
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      }
    )

    //2. sink to kafka
    dataStream.addSink(
      new FlinkKafkaProducer011[String]("localhost:9092", "sinkTest", new SimpleStringSchema())
    )

    env.execute("fileSinkTest")

  }
}
