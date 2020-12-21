package cn.majs.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @program flink-tutorial 
 * @description: 用传感器的例子，分别从集合、文件中获取数据
 * @author: mac 
 * @create: 2020/12/03 16:38 
 */
object Sensor {
  def main(args: Array[String]): Unit = {

    // 获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 从集合中获取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
    stream1.print("stream1: ").setParallelism(1)

    // 从文件中获取数据
    val stream2: DataStream[String] = env.readTextFile("src/main/resources/sendordata.txt")
    stream2.print("stream2: ").setParallelism(1)

    // 从kafka中获取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    stream3.print("stream3: ").setParallelism(1)

    // 从自定义数据源获取
    val stream4 = env.addSource( new MySensorSource() )
    stream4.print("stream4: ").setParallelism(1)

    env.execute("Sensor")
  }
}

// 定义样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)
