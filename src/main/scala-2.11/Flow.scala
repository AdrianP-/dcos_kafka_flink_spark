package dcos_kafka_flink_spark


case class Flow_ip4(eventid: String,
                    dst_ip4: String,
                    dst_port: String,
                    duration: String,
                    in_bytes: String,
                    in_packets: String,
                    out_bytes: String,
                    out_packets: String,
                    src_ip4: String,
                    src_port: String) extends Serializable


import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.native.JsonMethods._

class Flow(trace: String) extends Serializable{


  implicit val formats = DefaultFormats

  var flow_case : Flow_ip4 = parse(trace).extract[Flow_ip4]

  def getAllValuesFromString() = this.flow_case.productIterator.drop(1).map(_.asInstanceOf[String].toDouble).toList //Elimino el primer campo eventId
  //def getAllValuesFromString() = this.flow_case.productIterator.map(_.asInstanceOf[String].toDouble).toList

  def getKey() = this.flow_case.eventid

  def getIp() = this.flow_case.src_ip4

  def getFlow() : Flow_ip4 = flow_case

}


//case class Flow_ip6(eventid: String,
//                    dst_ip6: String,
//                    dst_port: String,
//                    duration: String,
//                    in_bytes: String,
//                    in_packets: String,
//                    out_bytes: String,
//                    out_packets: String,
//                    src_ip6: String,
//                    src_port: String)
