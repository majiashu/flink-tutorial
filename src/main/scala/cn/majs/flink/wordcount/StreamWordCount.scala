package cn.majs.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @program flink-tutorial 
 * @description: 流式WordCount
 *              .setParallelism(1)  设置并行度的优先级
 *                                先是看代码中指定的，然后代码中的配置文件
 *                                再是启动命令指定的，然后是集群配置文件的
 * @author: mac 
 * @create: 2020/12/02 15:28 
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 从外部获取参数
    // idea中配置运行参数方法； run-> Edit Configurations...
    // -->Configuration标签页的Program arguments中 写参数，例如-host localhost -port 9999
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = params.get("host")

    val port: Int = params.getInt("port")

    // 创建流环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 接受socket 文本流
    val textDStream: DataStream[String] = env.socketTextStream(host, port)

    // flatMap和Map 需要引入隐士转换  import org.apache.flink.api.scala._
    val result: DataStream[(String, Int)] = textDStream.flatMap(_.split(" ")).
      filter(_.nonEmpty).
      map((_, 1)).
      keyBy(0).
      sum(1)

    result.print().setParallelism(1) //一般输出，比如写入文件等将并行度设为1

    env.execute("StreamWordCount")
  }
}
