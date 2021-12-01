package com.brianjia.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * Batch processing for word count
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1. create context
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2. read the data
    val inputPath: String = "/Users/chunyangjia/IdeaProjects/FlinkTutorial/src/main/resources/hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    //3. data transform
    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) //based on the first element
      .sum(1) // sum based on the second element

    resultDataSet.print()
  }
}
