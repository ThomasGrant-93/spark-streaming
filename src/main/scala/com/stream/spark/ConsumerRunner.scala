package com.stream.spark

import com.stream.spark.configuration.{CassandraConfig, ConsumerConfig, KafkaConfig, SparkConfig}
import com.stream.spark.consumer.SparkKafkaConsumer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

object ConsumerRunner {
  private val logger: Logger = Logger.getLogger(getClass.getName)
  val config: Config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {
    logger.debug(s"${getClass.getName} maintenance with args: ${args.mkString(", ")}")
    val sparkConfig = SparkConfig(config)
    val kafkaConfig = KafkaConfig(config)
    val consumerConfig = ConsumerConfig(config)
    val cassandraConfig = CassandraConfig(config)

    logger.info(s"Spark Config: Master=${sparkConfig.master}, App Name=${sparkConfig.name}")
    logger.info(s"Kafka Config: Bootstrap Servers=${kafkaConfig.bootstrapServers}")

    val app = new SparkKafkaConsumer(sparkConfig, kafkaConfig, consumerConfig, cassandraConfig)
    app.run(args)
  }
}
