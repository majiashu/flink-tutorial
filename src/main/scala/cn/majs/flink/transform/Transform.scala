package cn.majs.flink.transform

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @program flink-tutorial
 * @description: flink的transform算子的介绍
 * @author: mac
 * @create: 2020/12/21 15:13
 */
object Transform {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val listDS: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))

    val mapDS: DataStream[Int] = listDS.map(line => line * 2)
    mapDS.print().setParallelism(1)

    env.execute("Transform")
  }

}
