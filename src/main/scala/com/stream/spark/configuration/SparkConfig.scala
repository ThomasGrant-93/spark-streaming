package com.stream.spark.configuration

import com.typesafe.config.Config

final case class SparkConfig(master: String, name: String)

object SparkConfig {
  def apply(config: Config): SparkConfig = {
    SparkConfig(
      config.getString("app.spark.consumer.master"),
      config.getString("app.spark.consumer.name")
    )
  }
}
