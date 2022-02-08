package com.brianjia.sinkTest

import com.brianjia.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

object JdbcSink {
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
      new MyJdbcSinkFunction()
    )

    env.execute("fileSinkTest")

  }
}

class MyJdbcSinkFunction extends RichSinkFunction[SensorReading] {

  //define database connection, precompiled
  var conn: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","password")
    insertStatement = conn.prepareStatement("insert into sensor(id, temp) values(?,?)")
    updateStatement = conn.prepareStatement("update sensor set temp = ? where id = ?")
  }

  override def invoke(value: SensorReading): Unit = {
    // 1. update if found, else insert
    updateStatement.setDouble(1, value.temperature)
    updateStatement.setString(2,value.id)
    updateStatement.execute()
    if (updateStatement.getUpdateCount == 0) {
      insertStatement.setString(1, value.id)
      insertStatement.setDouble(2, value.temperature)
      insertStatement.execute()
    }

  }

  override def close(): Unit = {
    insertStatement.close()
    updateStatement.close()
    conn.close()
  }
}
