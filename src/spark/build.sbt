name := "FraudDetectionStreaming"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "ml.dmlc" % "xgboost4j-spark_2.12" % "1.7.0", 
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
)

// Fix for deduplicate errors in assembly if we were using it, 
// but primarily fixes runtime issues with certain shaded libs if needed.
// For now, these are standard.
