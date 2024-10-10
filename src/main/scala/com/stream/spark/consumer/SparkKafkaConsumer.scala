package com.stream.spark.consumer

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.stream.spark.configuration.{CassandraConfig, ConsumerConfig, KafkaConfig, SparkConfig}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkKafkaConsumer(
                          sparkConfig: SparkConfig,
                          kafkaConfig: KafkaConfig,
                          consumerConfig: ConsumerConfig,
                          cassandraConfig: CassandraConfig
                        ) extends Serializable {

  @transient private val logger: Logger = Logger.getLogger(getClass.getName)

  private def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName(sparkConfig.name)
      .master(sparkConfig.master)
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.cassandra.connection.host", cassandraConfig.host)
      .config("spark.cassandra.connection.port", cassandraConfig.port)
      .config("spark.cassandra.auth.username", cassandraConfig.user)
      .config("spark.cassandra.auth.password", cassandraConfig.pass)
      .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
      .getOrCreate()
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
    val jsonSchema = new StructType()
      .add("timestamp", DoubleType, nullable = false)
      .add("device", StringType, nullable = false)
      .add("temp", DoubleType, nullable = false)
      .add("humd", DoubleType, nullable = false)
      .add("pres", DoubleType, nullable = false)

    logger.info(s"Step 2 parseKafkaMessages")

    kafkaStreamDF
      .withColumn("valueString", col("value").cast("string"))
      .select(from_json(col("valueString"), jsonSchema).as("data"))
      .select(
        from_unixtime(col("data.timestamp")).cast(TimestampType).as("timestamp"),
        col("data.device"),
        col("data.temp"),
        col("data.humd"),
        col("data.pres")
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
      logger.debug(s"${getClass.getName} running with args: ${args.mkString(", ")}")
      logger.debug(s"Spark Kafka consumer startup in ${System.currentTimeMillis()}")

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
            case e: Exception =>
              logger.error(s"Error writing batch $batchID to Cassandra", e)
          }
        }
        .trigger(Trigger.ProcessingTime(consumerConfig.triggerInterval))
        .outputMode("complete")
        .start()

      query.awaitTermination()

    } catch {
      case e: Exception =>
        logger.error("Error in Spark Kafka Consumer", e)
    } finally {
      logger.debug(s"Spark Kafka consumer shutdown in ${System.currentTimeMillis()}")
      spark.stop()
    }
  }
}
