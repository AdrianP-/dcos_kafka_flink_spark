package dcos_kafka_flink_spark


import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

import math._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.json4s.DefaultFormats
import dcos_kafka_flink_spark.Flow
import dcos_kafka_flink_spark.Flow_ip4
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object FlowConsumerSpark extends App {


  override def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Kafka Window Stream Flow distances")
    val sc =  new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    val topic_in = "benchmark_flow"
    val topic_out = "output"
    val kafkaServer = "10.10.40.1:9092"

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServer,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test_flowConsumerSpark"
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic_in)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    val flow = stream.map(_.value())
      .filter(!_.contains("src_ip6"))
      .map(record => {
        implicit val formats = DefaultFormats
        val flow  = new Flow(record).getFlow()
        (flow.src_ip4,flow)
      })
      .groupByKey()


    val props_out = new Properties()

    props_out.put("bootstrap.servers", kafkaServer)
    props_out.put("value.serializer", classOf[StringSerializer].getName)
    props_out.put("key.serializer", classOf[StringSerializer].getName)

    lazy val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props_out)

    flow.foreachRDD( rdd => {
      rdd.foreach{ case (key, input ) => {
        val distances = utils.calculateDistances(input)
        distances.map(x => {
          val obj = Map("eventid_1" -> x._1,
            "eventid_2" -> x._2,
            "distance" -> x._3,
            "timestamp" -> System.currentTimeMillis())
          val str_obj = scala.util.parsing.json.JSONObject(obj).toString()
          producer.send(new ProducerRecord[String, String](topic_out, str_obj))
        })
      }}
    })




    ssc.start()
    ssc.awaitTermination()
  }
}