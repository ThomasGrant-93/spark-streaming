package com.stream.spark.configuration

import com.typesafe.config.Config

final case class AppConfig(appName: String, appVer: String) {
  def name: String = s"${appName} - ${appVer}"
}

object AppConfig {
  def apply(config: Config): AppConfig = {
    AppConfig(
      config.getString("app.name"),
      config.getString("app.version")
    )
  }
}
