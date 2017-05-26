/**
  * Created by aportabales on 10/03/17.
  */
package dcos_kafka_flink_spark


import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import math._



object FlowConsumerFlink extends App {

  override def main(args: Array[String]) {
    val cores = 8

    val senv = StreamExecutionEnvironment.createLocalEnvironment(cores)
    //val senv = StreamExecutionEnvironment.createRemoteEnvironment("localhost",6123,"/home/aportabales/dcos_kafka_flink_spark/target/scala-2.11/dcos_kafka_flink_spark-assembly-1.0.jar")
    senv.setParallelism(cores)

    val topic_in = "benchmark_flow"
    val topic_out = "benchmark_flow_flink_10k_"+cores+"cores"
    val kafkaServer = "10.10.40.1:9092"

    val props_in = new Properties()
    props_in.setProperty("bootstrap.servers", kafkaServer)
    //props_in.setProperty("bootstrap.servers", "localhost:9092")
    props_in.setProperty("group.id", "test_flowConsumerFlink")

    val stream = senv.addSource(new FlinkKafkaConsumer010[String](topic_in, new SimpleStringSchema(), props_in))

    val flow = stream
      .filter(!_.contains("src_ip6"))
      .map(trace =>{

        implicit val formats = DefaultFormats
        val flow_case : Flow_ip4 = parse(trace).extract[Flow_ip4] //Instead of put this piece of code inside Flow class, Spark has a problem with the serializations and implicit. Flink works fine.

        new Flow(flow_case)
      })
      .keyBy(_.getIp())
      .timeWindow(Time.seconds(3))
      .apply{(
              key: String,
              window: TimeWindow,
              input: Iterable[Flow],
              out: Collector[List[(String, Long, String, Long, Double)]]) => {
                  val distances = utils.calculateDistances(input)
                  out.collect(distances)
                }
        }
      .flatMap(x => x)

    val props_out = new Properties()
    props_out.put("bootstrap.servers", kafkaServer)
    props_out.put("value.serializer", classOf[StringSerializer].getName)
    props_out.put("key.serializer", classOf[StringSerializer].getName)

    lazy val producer = new KafkaProducer[String,String](props_out)

    flow.map( x=>{
      val str_obj: String = utils.generateJson(x)
      producer.send(new ProducerRecord[String, String](topic_out, str_obj))
      str_obj
    })

    senv.execute("Kafka Window Stream Flow distances")
  }
}
