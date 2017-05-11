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

  def getCombinations(lists : Iterable[Flow_ip4]) = {
    lists.toList.combinations(2).map(x=> (x(0),x(1))).toList
  }

  def getAllValuesFromString(flow_case : Flow_ip4) = flow_case.productIterator.drop(1).map(_.asInstanceOf[String].toDouble).toList

  def calculateDistances(input: Iterable[Flow_ip4]): List[(String, String, Double)] = {
    val combinations: List[(Flow_ip4, Flow_ip4)] = getCombinations(input)
    val distances = combinations.map{
      case(f1,f2) => (f1.eventid,f2.eventid,euclideanDistance(getAllValuesFromString(f1),getAllValuesFromString(f2)))}
    distances.sortBy(_._3)
  }

  def generateJson(x: (String, String, Double)): String = {
    val obj = Map("eventid_1" -> x._1,
      "eventid_2" -> x._2,
      "distance" -> x._3,
      "timestamp" -> System.currentTimeMillis())
    val str_obj = scala.util.parsing.json.JSONObject(obj).toString()
    str_obj
  }
}