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

class Flow(flow_case : Flow_ip4) extends Serializable {

  val create = System.currentTimeMillis()

  def getKey() = this.flow_case.eventid

  def getIp() = this.flow_case.src_ip4

  def getFlow() : Flow_ip4 = flow_case

  def getCreate() : Long = create

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
