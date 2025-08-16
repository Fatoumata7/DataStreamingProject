import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object MainApp {

  def main(args: Array[String]): Unit = {

    val kafkaBroker = "kafka-broker:9093"
    val transactionTopic = "transactions"
    val fraudAlertsTopic = "fraud-alerts"
    val parquetOutputPath = "output/fraud_alerts"

    val spark = SparkSession.builder()
      .appName("KafkaSparkTransactionProcessor")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val transactionSchema = new StructType()
      .add("user_id", StringType)
      .add("transaction_id", StringType)
      .add("amount", DoubleType)
      .add("currency", StringType)
      .add("timestamp", StringType)
      .add("location", StringType)
      .add("method", StringType)

    val inputStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", transactionTopic)
      .option("startingOffsets", "latest")
      .load()

    val transactionsDF = inputStream
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), transactionSchema).as("data"))
      .select("data.*")
      .withColumn("event_time", to_timestamp(col("timestamp")))

    // === RULE FUNCTIONS ===

    // --- Rule 1: High-value transactions over 3500
    def highValueRule(df: DataFrame): DataFrame =
      df.filter(col("amount") > 3500)
        .withColumn("fraud_type", lit("HIGH_VALUE"))
        .withColumn("fraud_reason", concat(lit("High transaction amount: "), col("amount")))
        .withColumn("alert_timestamp", current_timestamp())
        .select("user_id", "transaction_id", "amount", "currency", "timestamp", 
                "location", "method", "fraud_type", "fraud_reason", "alert_timestamp")

    // --- Rule 2: More than 3 transactions from the same user within 1 minute
    def multipleTransactionsRule(df: DataFrame): DataFrame =
      df.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col("user_id"))
        .count()
        .filter(col("count") > 3)
        .select(
          col("user_id"),
          col("window.start").alias("window_start"),
          col("window.end").alias("window_end"),
          col("count").alias("txn_count")
        )
        .withColumn("fraud_type", lit("MULTIPLE_TX"))
        .withColumn("fraud_reason", concat(lit("User has made "), col("txn_count"), lit(" transactions within 1 minute")))
        .withColumn("alert_timestamp", current_timestamp())
        .select("user_id", "window_start", "window_end", "txn_count", "fraud_type", "fraud_reason", "alert_timestamp")

    // --- Rule 3: Transactions in multiple locations within 2 minutes
    def multipleLocationsRule(df: DataFrame): DataFrame =
      df.withWatermark("event_time", "4 minutes")
        .groupBy(window(col("event_time"), "2 minutes"), col("user_id"))
        .agg(approx_count_distinct(col("location")).alias("city_count"))
        .filter(col("city_count") > 1)
        .select(
          col("user_id"),
          col("window.start").alias("window_start"),
          col("window.end").alias("window_end"),
          col("city_count")
        )
        .withColumn("fraud_type", lit("MULTIPLE_LOC"))
        .withColumn("fraud_reason", concat(lit("User has made transactions in "), col("city_count"),  lit(" different locations within 2 minutes")))
        .withColumn("alert_timestamp", current_timestamp())
        .select("user_id", "window_start", "window_end", "city_count", "fraud_type", "fraud_reason", "alert_timestamp")

    // --- Rule 4: Multiple small PayPal transactions in a short period of time
    def smallPaypalTransactionsRule(df: DataFrame): DataFrame =
      df.filter(col("method") === "paypal" && col("amount") < 50)
        .withWatermark("event_time", "8 minutes")
        .groupBy(window(col("event_time"), "4 minutes"), col("user_id"))
        .count()
        .filter(col("count") > 3)
        .select(
          col("user_id"),
          col("window.start").alias("window_start"),
          col("window.end").alias("window_end"),
          col("count").alias("txn_count")
        )
        .withColumn("fraud_type", lit("SMALL_PAYPAL_TX"))
        .withColumn("fraud_reason", concat(lit("User made "), col("txn_count"), lit(" small PayPal transactions in 4 minutes")))
        .withColumn("alert_timestamp", current_timestamp())
        .select("user_id", "window_start", "window_end", "txn_count", "fraud_type", "fraud_reason", "alert_timestamp")


    // === OUTPUT FUNCTIONS ===

    def writeToConsole(df: DataFrame, name: String, checkpoint: String): Unit =
      df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", false)
        .option("checkpointLocation", checkpoint)
        .trigger(Trigger.ProcessingTime("5 minutes"))
        .queryName(name)
        .start()

    def writeToKafka(df: DataFrame, checkpoint: String, name: String): Unit =
      df.selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBroker)
        .option("topic", fraudAlertsTopic)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("5 minutes"))
        .queryName(name)
        .start()

    def writeToParquet(df: DataFrame, subPath: String, checkpoint: String, name: String): Unit =
      df.writeStream
        .format("parquet")
        .option("path", s"$parquetOutputPath/$subPath")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("5 minutes"))
        .queryName(name)
        .start()

    // === APPLY RULES AND WRITE TO DESTINATIONS ===

    val rule1Console = highValueRule(transactionsDF)
    println("(schema) Rule 1 : High Value Transactions")
    rule1Console.printSchema()

    val rule2Console = multipleTransactionsRule(transactionsDF)
    println("(schema) Rule 2 : Multiple Transactions")
    rule2Console.printSchema()

    val rule3Console = multipleLocationsRule(transactionsDF)
    println("(schema) Rule 3 : Multiple Locations")
    rule3Console.printSchema()

    val rule4Console = smallPaypalTransactionsRule(transactionsDF)
    println("(schema) Rule 4 : Small PayPal Transactions")
    rule4Console.printSchema()

    // --- Console outputs
    writeToConsole(rule1Console, "HighValueConsole", "/tmp/spark-console-high")
    writeToConsole(rule2Console, "MultipleTxConsole", "/tmp/spark-console-multiple")
    writeToConsole(rule3Console, "MultipleLocConsole", "/tmp/spark-console-location")
    writeToConsole(rule4Console, "SmallPaypalConsole", "/tmp/spark-console-smallpaypal")

    val rule1Kafka = highValueRule(transactionsDF)
    val rule2Kafka = multipleTransactionsRule(transactionsDF)
    val rule3Kafka = multipleLocationsRule(transactionsDF)
    val rule4Kafka = smallPaypalTransactionsRule(transactionsDF)

    // --- Kafka outputs
    writeToKafka(rule1Kafka, "/tmp/spark-kafka-high", "HighValueKafka")
    writeToKafka(rule2Kafka, "/tmp/spark-kafka-multiple", "MultipleTxKafka")
    writeToKafka(rule3Kafka, "/tmp/spark-kafka-location", "MultipleLocKafka")
    writeToKafka(rule4Kafka, "/tmp/spark-kafka-smallpaypal", "SmallPaypalKafka")

    val rule1Parquet = highValueRule(transactionsDF)
    val rule2Parquet = multipleTransactionsRule(transactionsDF)
    val rule3Parquet = multipleLocationsRule(transactionsDF)
    val rule4Parquet = smallPaypalTransactionsRule(transactionsDF)

    // --- Parquet outputs
    writeToParquet(rule1Parquet, "high_value", "/tmp/spark-parquet-high", "HighValueParquet")
    writeToParquet(rule2Parquet, "multiple_tx", "/tmp/spark-parquet-multiple", "MultipleTxParquet")
    writeToParquet(rule3Parquet, "multiple_loc", "/tmp/spark-parquet-location", "MultipleLocParquet")
    writeToParquet(rule4Parquet, "small_paypal", "/tmp/spark-parquet-smallpaypal", "SmallPaypalParquet")

    spark.streams.awaitAnyTermination()
  }
}
