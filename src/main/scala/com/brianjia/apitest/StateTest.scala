package com.brianjia.apitest

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import javax.xml.crypto.KeySelector

object StateTest {
  def main(args: Array[String]): Unit = {
    //1. start the env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2. read in the file
    val inputStream = env.socketTextStream("localhost", 7777);

    //3. tranform
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    //4. alarming when the sensor readings jumps more than 10 degrees
    val alertStream = dataStream
      .keyBy("id")
//      .flatMap(new TempChangeAlert(10.0))


    alertStream.print()
    env.execute("state test")
  }
}
//rich flap map function
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  //define a state to store the previous temp
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("last-temp", classOf[Double])
  )
  var updated = true

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    // get last temperature
    val lastTemp = lastTempState.value()
    // compare to the current temperature

    if (updated) {
      //update the temperature
      lastTempState.update(in.temperature)
    } else {
      val diff = (in.temperature - lastTemp).abs
      if (diff > threshold) {
        collector.collect((in.id, lastTemp, in.temperature))
      }
      updated = false;
    }
  }
}

//keyed state must be defined in the rich function class
//it will needs runtime context
class MyRichMapper2 extends RichMapFunction[SensorReading, String]{

  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(
    new ListStateDescriptor[Int]("listState", classOf[Int])
  )

  override  def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("valueState", classOf[Double])
      )
  }
  override def map(in: SensorReading): String = {
    // state write/read
    val myValue = valueState.value()
    valueState.update(in.temperature)
    listState.add(1)

    in.id
  }
}