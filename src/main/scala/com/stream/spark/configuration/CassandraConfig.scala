package com.stream.spark.configuration

import com.typesafe.config.Config

case class CassandraConfig(
                            user: String,
                            pass: String,
                            host: String,
                            port: String,
                            keyspace: String,
                            table: String
                          ) {}

object CassandraConfig {
  def apply(config: Config): CassandraConfig = {
    CassandraConfig(
      config.getString("app.cassandra.user"),
      config.getString("app.cassandra.pass"),
      config.getString("app.cassandra.host"),
      config.getString("app.cassandra.port"),
      config.getString("app.cassandra.keyspace"),
      config.getString("app.cassandra.table")
    )
  }
}
