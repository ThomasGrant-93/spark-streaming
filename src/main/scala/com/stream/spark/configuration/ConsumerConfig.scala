package com.stream.spark.configuration

import com.typesafe.config.Config

final case class ConsumerConfig(
                                 master: String,
                                 name: String,
                                 interval: String
                               ) {
  def appName: String = name

  def triggerInterval: String = interval
}

object ConsumerConfig {
  def apply(config: Config): ConsumerConfig = {
    ConsumerConfig(
      config.getString("app.spark.consumer.master"),
      config.getString("app.spark.consumer.name"),
      config.getString("app.spark.consumer.interval")
    )
  }
}
