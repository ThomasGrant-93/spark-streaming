package com.stream.spark.configuration

import com.typesafe.config.Config

final case class KafkaConfig(
                              host: String,
                              port: String,
                              topic: String,
                              offset: String
                            ) {
  def bootstrapServers: String = s"${host}:${port}"

  def startingOffsets: String = offset
}

object KafkaConfig {
  def apply(config: Config): KafkaConfig = {
    KafkaConfig(
      config.getString("app.kafka.host"),
      config.getString("app.kafka.port"),
      config.getString("app.kafka.topic"),
      config.getString("app.kafka.offset")
    )
  }
}
