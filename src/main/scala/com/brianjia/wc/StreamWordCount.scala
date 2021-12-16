package com.brianjia.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * Streaming for word count
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //1. create context, same as batch processing
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(8)
    //read configuration files as input params
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")

//    print(paramTool.toMap)
    //2. receive a web socket text stream
    val inputDataStream: DataStream[String] = env.socketTextStream(host,port)
    // transformation and statistics

    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    // start a process and wait for data
    // This is a streaming service, data hasn't come yet!!!
    env.execute("stream word count")

    //bash: nc -lk 7777 to start a server on port 7777
  }
}
