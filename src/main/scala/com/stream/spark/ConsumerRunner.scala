package com.stream.spark

import com.stream.spark.configuration._
import com.stream.spark.consumer.SparkKafkaConsumer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

object ConsumerRunner {
  private val logger: Logger = Logger.getLogger(getClass.getName)
  private val config: Config = ConfigFactory.load()

  private def loadConfigs(): (AppConfig, SparkConfig, KafkaConfig, ConsumerConfig, CassandraConfig) = {
    (
      AppConfig(config),
      SparkConfig(config),
      KafkaConfig(config),
      ConsumerConfig(config),
      CassandraConfig(config)
    )
  }

  def main(args: Array[String]): Unit = {
    logger.info(s"${getClass.getName} maintenance with args: ${args.mkString(", ")}")
    val (appConfig, sparkConfig, kafkaConfig, consumerConfig, cassandraConfig) = loadConfigs()

    logger.info(s"App Config: appName=${appConfig.appName}, appVer=${appConfig.appVer}")
    logger.info(s"Spark Config: Master=${sparkConfig.master}, App Name=${sparkConfig.name}")
    logger.info(s"Kafka Config: Bootstrap Servers=${kafkaConfig.bootstrapServers}")

    new SparkKafkaConsumer(sparkConfig, kafkaConfig, consumerConfig, cassandraConfig).run(args)
  }
}
