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
                    src_port: String)


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


//val trace_str = "{\"_id\": \"a\", \"dst_ip4\": 6436, \"dst_port\": 849, \"duration\": 778, \"in_bytes\": 345, \"in_packets\": 54, \"out_bytes\": 190, \"out_packets\": 688, \"src_ip4\": 623, \"src_port\": 276}"
//val trace_str1 = "{\"_id\": \"b\", \"dst_ip4\": 6436, \"dst_port\": 849, \"duration\": 778, \"in_bytes\": 345, \"in_packets\": 54, \"out_bytes\": 190, \"out_packets\": 688, \"src_ip4\": 623, \"src_port\": 276}"
//val trace_str2 = "{\"_id\": \"c\", \"dst_ip4\": 6436, \"dst_port\": 849, \"duration\": 778, \"in_bytes\": 345, \"in_packets\": 54, \"out_bytes\": 190, \"out_packets\": 688, \"src_ip4\": 623, \"src_port\": 276}"
//val trace_str3 = "{\"_id\": \"e\", \"dst_ip4\": 6436, \"dst_port\": 849, \"duration\": 778, \"in_bytes\": 345, \"in_packets\": 54, \"out_bytes\": 190, \"out_packets\": 688, \"src_ip4\": 623, \"src_port\": 276}"
//val flow = new Flow(trace_str)
//val flow1 = new Flow(trace_str1)
//val flow2 = new Flow(trace_str2)
//val flow3 = new Flow(trace_str3)
//val flow = Flow.getAllValuesFromString(trace_str)
//Flow.getKey(trace_str)

//val s = """
//{"eventid":1665615092977835,"dvc_version":"3.2","aproto":"failed","src_ip4":168099981,"dst_ip4":168100351,"eventtime":1490107379,"out_packets":1,"type":"flow","dvc_time":1490107411,"dst_ip":"10.5.1.255","src_ip":"10.5.0.141","duration":0,"in_bytes":0,"conn_state":"new","@version":"1","out_bytes":86,"dvc_type":"suricata","additional_atts":"{\"reason\":\"timeout\"}","raw":"{\"app_proto\":\"failed\",\"src_ip\":\"10.5.0.141\",\"src_port\":57621,\"event_type\":\"flow\",\"@timestamp\":\"2017-03-21T14:43:31.335Z\",\"flow_id\":1665615092977835,\"dest_ip\":\"10.5.1.255\",\"proto\":\"UDP\",\"host\":\"mgda-inspector\",\"@version\":\"1\",\"dest_port\":57621,\"flow\":{\"reason\":\"timeout\",\"pkts_toserver\":1,\"start\":\"2017-03-21T14:42:59.004267+0000\",\"bytes_toclient\":0,\"end\":\"2017-03-21T14:42:59.004267+0000\",\"state\":\"new\",\"bytes_toserver\":86,\"pkts_toclient\":0,\"age\":0},\"timestamp\":\"2017-03-21T14:43:31.002441+0000\"}","in_packets":0,"nproto":"UDP","src_port":57621,"@timestamp":"2017-03-21T14:42:59.000Z","dst_port":57621,"category":"informational"}
//"""
//val flow = new Flow(s)
