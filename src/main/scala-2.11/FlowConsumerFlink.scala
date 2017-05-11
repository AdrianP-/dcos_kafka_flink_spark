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

import math._



object FlowConsumerFlink extends App {

  override def main(args: Array[String]) {

    val senv = StreamExecutionEnvironment.createLocalEnvironment(2)

    val topic_in = "benchmark_flow"
    val topic_out = "output"
    val kafkaServer = "10.10.40.1:9092"

    val props_in = new Properties()
    props_in.setProperty("bootstrap.servers", kafkaServer)
    //props_in.setProperty("bootstrap.servers", "localhost:9092")
    props_in.setProperty("group.id", "test_flowConsumerFlink")

    val stream = senv.addSource(new FlinkKafkaConsumer010[String](topic_in, new SimpleStringSchema(), props_in))


    val flow = stream
      .filter(!_.contains("src_ip6"))
      .map(trace =>{
      new Flow(trace).getFlow()
    })
      .keyBy(_.src_ip4)
      .timeWindow(Time.seconds(3))
      .apply{(
              key: String,
              window: TimeWindow,
              input: Iterable[Flow_ip4],
              out: Collector[List[(String,String, Double)]]) => {
                  val distances: List[(String, String, Double)] = utils.calculateDistances(input)
                  out.collect(distances)
                }
        }
      .flatMap(x => x)
      .map(x=>(System.currentTimeMillis(),x))


//    flow.writeAsText("/tmp/outputFlink.log")

    val props_out = new Properties()
    props_out.put("bootstrap.servers", kafkaServer)
    props_out.put("value.serializer", classOf[StringSerializer].getName)
    props_out.put("key.serializer", classOf[StringSerializer].getName)

    lazy val producer = new KafkaProducer[String,String](props_out)

    flow.map( x=>{
      val obj =Map("eventid_1"-> x._2._1,
                   "eventid_2"-> x._2._2,
                  "distance" -> x._2._3,
                  "timestamp" -> x._1)
      val str_obj = scala.util.parsing.json.JSONObject(obj).toString()

      producer.send(new ProducerRecord[String, String](topic_out, str_obj))
    })

    senv.execute("Kafka Window Stream Flow distances")
  }
}
