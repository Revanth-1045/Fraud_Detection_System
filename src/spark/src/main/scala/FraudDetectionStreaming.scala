import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

object FraudDetectionStreaming {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val logger = Logger.getLogger("FraudDetectionStreaming")

    val spark = SparkSession.builder()
      .appName("FraudDetectionStreaming")
      .getOrCreate()

    import spark.implicits._

    val KAFKA_BOOTSTRAP_SERVERS = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    val KAFKA_TOPIC = sys.env.getOrElse("KAFKA_TOPIC", "transactions")

    // Mock User Averages (Broadcast)
    // using a simple map for demonstration as per python script
    val userAvgsMock = (1 to 100).map(i => i -> 100.0).toMap
    val broadcastAvgs = spark.sparkContext.broadcast(userAvgsMock)

    // Schema
    val schema = new StructType()
      .add("transaction_id", StringType)
      .add("user_id", IntegerType)
      .add("amount", DoubleType)
      .add("terminal_id", IntegerType)
      .add("timestamp", TimestampType)
      .add("tx_count_1h", IntegerType)
      .add("avg_diff_ratio", DoubleType) 
      .add("is_high_risk_hr", IntegerType)
      .add("target_fraud", IntegerType)

    // Read Stream
    val dfKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .load()

    val dfParsed = dfKafka.select(from_json($"value".cast("string"), schema).as("data"))
      .select("data.*")

    // Feature Engineering
    
    // 1. is_high_risk_hr (Recalculate to verify)
    val dfEnriched = dfParsed.withColumn("calc_high_risk_hr", 
      when(hour($"timestamp") >= 1 && hour($"timestamp") <= 5, 1).otherwise(0)
    )

    // 2. Window Aggregation
    val dfWindowed = dfEnriched
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        window($"timestamp", "1 hour", "1 minute"),
        $"user_id"
      )
      .agg(
        count("*").as("calc_tx_count_1h"),
        last($"amount").as("amount"),
        last($"transaction_id").as("transaction_id"),
        last($"timestamp").as("timestamp"),
        last($"tx_count_1h").as("input_tx_count_1h"),
        last($"is_high_risk_hr").as("input_is_high_risk_hr"),
         last($"calc_high_risk_hr").as("calc_high_risk_hr")
      )

    // Processing Logic (ForeachBatch)
    val query = dfWindowed.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        if (!batchDF.isEmpty) {
          logger.warn(s"Processing batch $batchId with ${batchDF.count()} records")
          
          // We can't use the simple python-map approach on a Dataset easily without collecting or using UDFs.
          // We will use a UDF for the broadcast lookup to keep it distributed.
          
          val lookupAvg = udf((userId: Int) => broadcastAvgs.value.getOrElse(userId, 100.0))
          
          val processed = batchDF
            .withColumn("user_avg", lookupAvg($"user_id"))
            .withColumn("calc_avg_diff_ratio", $"amount" / $"user_avg")
            .withColumn("final_tx_count_1h", coalesce($"input_tx_count_1h", $"calc_tx_count_1h")) // Prefer input if available? Python script logic was ambiguous, let's use calc.
            .withColumn("is_fraud_heuristic", 
               // Simple Heuristic to prove pipeline works (since loading python XGB model in Scala is complex without PMML/ONNX)
               when(($"amount" > 10000) || ($"calc_high_risk_hr" === 1 && $"calc_tx_count_1h" > 5), 1).otherwise(0)
            )

          val frauds = processed.filter($"is_fraud_heuristic" === 1)
          
          if (!frauds.isEmpty) {
             logger.warn(s"FRAUD DETECTED: ${frauds.count()} transactions!")
             frauds.select("transaction_id", "user_id", "amount", "is_fraud_heuristic").show(false)
          }
        }
      }
      .start()

    query.awaitTermination()
  }
}
