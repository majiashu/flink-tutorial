package cn.majs.flink.wordcount

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @program flink-tutorial
 * @description: flink的WordCount 批处理 案例
 * @author: mac
 * @create: 2020/12/02 14:34
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    // 批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val inputPath: String = "src/main/resources/word.txt"

    val inputDS: DataSet[String] = env.readTextFile(inputPath)

    // 先分词然后再分组，再求和
    val result: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    result.print()

  }



}
