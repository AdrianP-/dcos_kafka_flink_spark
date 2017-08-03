/**
  * Created by aportabales on 11/05/17.
  */

package dcos_kafka_flink_spark

import dcos_kafka_flink_spark.Flow_ip4
import scala.math._



object utils{

  def euclideanDistance(xs: List[Double], ys: List[Double]) = {
    sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
  }

  def getCombinations(lists : Iterable[Flow]) = {
    lists.toList.combinations(2).map(x=> (x(0),x(1))).toList
  }

  def getValuesForDistance(flow : Flow) = flow.getFlow().productIterator.drop(1).map(_.asInstanceOf[String].toDouble).toList

  def calculateDistances(input: Iterable[Flow]) = {
    val combinations: List[(Flow, Flow)] = getCombinations(input)
    val distances = combinations.map{
      case(f1,f2) => (f1.getKey(),f1.getCreate(),f2.getKey(),f2.getCreate(),euclideanDistance(getValuesForDistance(f1),getValuesForDistance(f2)))}
    distances.sortBy(_._3)
  }

  def generateJson(x: (String, Long, String, Long, Double )): String = {
    val obj = Map("eventid_1" -> x._1,
      "ts_flow1" -> x._2,
      "eventid_2" -> x._3,
      "ts_flow2" -> x._4,
      "distance" -> x._5,
      "ts_output" -> System.currentTimeMillis())
    val str_obj = scala.util.parsing.json.JSONObject(obj).toString()
    str_obj
  }
}