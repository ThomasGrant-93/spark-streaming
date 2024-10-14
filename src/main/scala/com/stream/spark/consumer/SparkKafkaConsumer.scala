package com.stream.spark.consumer

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.stream.spark.configuration.{CassandraConfig, ConsumerConfig, KafkaConfig, SparkConfig}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkKafkaConsumer(
                          sparkConfig: SparkConfig,
                          kafkaConfig: KafkaConfig,
                          consumerConfig: ConsumerConfig,
                          cassandraConfig: CassandraConfig
                        ) extends Serializable {

  @transient private val logger: Logger = Logger.getLogger(getClass.getName)

  private def createSparkSession(): SparkSession = {
    val cassConfigsMap = Map(
      "spark.cassandra.connection.host" -> cassandraConfig.host,
      "spark.cassandra.connection.port" -> cassandraConfig.port,
      "spark.cassandra.auth.username" -> cassandraConfig.user,
      "spark.cassandra.auth.password" -> cassandraConfig.pass,
      "spark.cassandra.output.consistency.level" -> "LOCAL_QUORUM"
    )

    val builder = SparkSession.builder()
      .appName(sparkConfig.name)
      .master(sparkConfig.master)
      .config("spark.streaming.stopGracefullyOnShutdown", "true")

    cassConfigsMap.foreach { case (key, value) => builder.config(key, value) }

    builder.getOrCreate()
  }

  private def createKafkaStream(spark: SparkSession): DataFrame = {

    logger.info(s"Step 1 createKafkaStream")

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("subscribe", kafkaConfig.topic)
      .option("failOnDataLoss", "false")
      .option("startingOffsets", kafkaConfig.startingOffsets)
      .load()
  }

  private def parseKafkaMessages(kafkaStreamDF: DataFrame): DataFrame = {
    val jsonSchema = StructType(
      Seq(
        StructField("timestamp", DoubleType, nullable = false),
        StructField("device", StringType, nullable = false),
        StructField("temp", DoubleType, nullable = false),
        StructField("humd", DoubleType, nullable = false),
        StructField("pres", DoubleType, nullable = false)
      )
    )

    logger.info(s"Step 2 parseKafkaMessages")

    kafkaStreamDF
      .select(from_json(col("value").cast("string"), jsonSchema).as("json"))
      .select(
        from_unixtime(col("json.timestamp")).cast(TimestampType).as("timestamp"),
        col("json.device"),
        col("json.temp"),
        col("json.humd"),
        col("json.pres")
      )
  }

  private def aggregateData(eventsDF: DataFrame): DataFrame = {
    val makeUUID: UserDefinedFunction = udf(() => Uuids.timeBased().toString)

    logger.info(s"Step 3 aggregateData")

    val aggregatedDF = eventsDF
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        col("device"),
        window(col("timestamp"), "10 seconds", "5 seconds")
      )
      .agg(
        avg("temp").alias("temp"),
        avg("humd").alias("humd"),
        avg("pres").alias("pres")
      )

    aggregatedDF
      .withColumn("uuid", makeUUID())
      .select(
        col("device"),
        col("temp"),
        col("humd"),
        col("pres"),
        col("window.start").alias("period_start"),
        col("window.end").alias("period_end"),
        col("uuid")
      )
  }

  def run(args: Array[String]): Unit = {
    val spark = createSparkSession()

    try {
      logger.info(s"${getClass.getName} running with args: ${args.mkString(", ")}")
      logger.info(s"Spark Kafka consumer startup in ${System.currentTimeMillis()}")

      val kafkaStreamDF = createKafkaStream(spark)
      val eventsDF = parseKafkaMessages(kafkaStreamDF)
      val summaryDF = aggregateData(eventsDF)

      val query: StreamingQuery = summaryDF
        .writeStream
        .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
          try {
            logger.info(s"Writing to Cassandra for batch $batchID")
            batchDF.write
              .format("org.apache.spark.sql.cassandra")
              .option("keyspace", cassandraConfig.keyspace)
              .option("table", cassandraConfig.table)
              .mode("append")
              .save()
            logger.info(s"Writing success $batchID")
          } catch {
            case ex: Exception =>
              logger.error(s"Error writing batch $batchID to Cassandra", ex)
          }
        }
        .trigger(Trigger.ProcessingTime(consumerConfig.triggerInterval))
        .outputMode("complete")
        .start()

      query.awaitTermination()

    } catch {
      case ex: Exception =>
        logger.error("Error in Spark Kafka Consumer", ex)
    } finally {
      logger.debug(s"Spark Kafka consumer shutdown in ${System.currentTimeMillis()}")
      spark.stop()
    }
  }
}
