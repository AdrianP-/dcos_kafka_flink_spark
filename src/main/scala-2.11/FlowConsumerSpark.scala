package dcos_kafka_flink_spark


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

import math._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.json4s.DefaultFormats


object FlowConsumerSpark extends App {


  def euclideanDistance(xs: List[Double], ys: List[Double]) = {
    sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
  }

  def getCombinations(lists : RDD[Flow_ip4]) = {
    lists.toLocalIterator.toList.combinations(2).map(x=> (x(0),x(1))).toList
  }

  def getAllValuesFromString(flow_case : Flow_ip4) = flow_case.productIterator.drop(1).map(_.asInstanceOf[String].toDouble).toList

  def calculateDistances(input: RDD[Flow_ip4]): List[(String, String, Double)] = {
    val combinations = getCombinations(input)
    val distances = combinations.map{
        case(f1,f2) => (f1.eventid,f2.eventid,euclideanDistance(getAllValuesFromString(f1),getAllValuesFromString(f2)))}
    distances.sortBy(_._3)
  }

  override def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Flow Consumer")
    val sc =  new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val topic_in = "benchmark_flow"
    val topic_out = "output"


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.10.40.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic_in)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    stream.map(_.value())
      .filter(!_.contains("src_ip6"))
      .map(record => {
        //Como json4s no es serializable no puedo instanciar Flow
        implicit val formats = DefaultFormats
        new Flow(record).getFlow()
      })
      .foreachRDD( x=> {
        val distances = this.calculateDistances(x)
        distances.map(x =>println((System.currentTimeMillis(),x)))
      })



    //stream.count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}