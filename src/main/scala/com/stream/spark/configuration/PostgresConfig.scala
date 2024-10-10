package com.stream.spark.configuration

import com.typesafe.config.Config

final case class PostgresConfig(host: String, port: String, db: String, user: String, pass: String) {
  def url: String = s"jdbc:postgresql://${host}:${port}/${db}"
}

object PostgresConfig {
  def apply(config: Config): PostgresConfig = {
    PostgresConfig(
      config.getString("app.database.host"),
      config.getString("app.database.port"),
      config.getString("app.database.db"),
      config.getString("app.database.user"),
      config.getString("app.database.pass")
    )
  }
}
