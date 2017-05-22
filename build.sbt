import sbtassembly.Plugin.AssemblyKeys._

name := "dcos_kafka_flink_spark"

version := "1.0"


scalaVersion := "2.11.8"

val flinkVersion = "1.2.0"
val sparkVersion = "2.1.1"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion
//libraryDependencies += "org.elasticsearch" % "elasticsearch-spark" % sparkVersion


libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion
libraryDependencies += "org.apache.flink" %% "flink-clients" % flinkVersion
libraryDependencies += "org.apache.flink" %% "flink-runtime" % flinkVersion
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion
libraryDependencies += "org.apache.flink" %% "flink-ml" % flinkVersion
//libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-base" % flinkVersion


libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.5.1"

val jacksonVersion = "2.6.0"
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion

dependencyOverrides ++= Set("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.1")

//libraryDependencies += "org.apache.kafka" %% "kafka" % "0.9.0.0"

assemblySettings


mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}


    