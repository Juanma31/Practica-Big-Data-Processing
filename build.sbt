name := "project-solution"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVersion = "2.4.7"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.2.16"
)

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "services", _@_*) => MergeStrategy.concat
  case PathList("META-INF", xs) if xs.endsWith(".SF") || xs.endsWith(".DSA") || xs.endsWith(".RSA") => MergeStrategy.discard
  case PathList("META-INF", _@_*) => MergeStrategy.first
  case _ => MergeStrategy.first
}