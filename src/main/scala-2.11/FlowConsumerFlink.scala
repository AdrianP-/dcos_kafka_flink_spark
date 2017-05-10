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


import math._






object FlowConsumerFlink extends App {

  def euclideanDistance(xs: List[Double], ys: List[Double]) = {
    sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
  }

  def getCombinations(lists : Iterable[Flow]) = {
    lists.toList.combinations(2).map(x=> (x(0),x(1))).toList
  }


  def calculateDistances(input: Iterable[Flow]): List[(String, String, Double)] = {
    val combinations: List[(Flow, Flow)] = getCombinations(input)
    val distances = combinations.map{
        case(f1,f2) => (f1.getKey().toString,f2.getKey().toString,euclideanDistance(f1.getAllValuesFromString(),f2.getAllValuesFromString()))}
    distances.sortBy(_._3)
  }

  override def main(args: Array[String]) {

    val senv = StreamExecutionEnvironment.createLocalEnvironment(4)

    val topic_in = "benchmark_flow"
    val topic_out = "output"

    val props_in = new Properties()
    props_in.setProperty("bootstrap.servers", "10.10.40.1:9092")
    //props_in.setProperty("bootstrap.servers", "localhost:9092")
    props_in.setProperty("group.id", "flink_"+topic_in)

    val stream = senv.addSource(new FlinkKafkaConsumer010[String](topic_in, new SimpleStringSchema(), props_in))

    //stream.print

    val flow = stream
      .filter(!_.contains("src_ip6"))
      .map(trace =>{
      new Flow(trace)
    })
      .keyBy(_.getIp())
      .timeWindow(Time.seconds(5))
      .apply{(
              key: String,
              window: TimeWindow,
              input: Iterable[Flow],
              out: Collector[List[(String,String, Double)]]) => {
                  val distances: List[(String, String, Double)] = calculateDistances(input)
                  out.collect(distances)
                }
        }
      .flatMap(x => x)
      .map(x=>(System.currentTimeMillis(),x))


//    flow.writeAsText("/tmp/outputFlink.log")
      flow.print()
//    senv.execute("Kafka Window Stream WordCount")



//    flow.map( x=>{
//      val props_out = new Properties()
//      //props_out.put("bootstrap.servers", "localhost:9092")
//      props_out.put("bootstrap.servers", "10.10.40.1:9092")
//      props_out.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//      props_out.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//      val producer = new KafkaProducer[String,String](props_out)
//
//
//      val obj =Map("eventid_1"-> x._1,
//                   "eventid_2"-> x._2,
//                  "distance" -> x._3)
//      val str_obj = scala.util.parsing.json.JSONObject(obj).toString()
//      producer.send(new ProducerRecord(topic_out,str_obj))
//      //Solo hay 1 particion
//    } )

    senv.execute("Kafka Window Stream WordCount")
  }
}
