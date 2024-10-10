ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

ThisBuild / organization := "com.stream.spark"

val sparkVersion = "3.2.4"

lazy val root = (project in file("."))
        .enablePlugins(AssemblyPlugin)
        .settings(
            name := "spark-streaming",
            libraryDependencies ++= Seq(
                "com.typesafe" % "config" % "1.4.3",
                "org.apache.kafka" % "kafka-clients" % "3.8.0",
                "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
                "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
                "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
                "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
                "com.github.jnr" % "jnr-posix" % "3.1.19"
            ),
            libraryDependencySchemes += "com.github.luben" % "zstd-jni" % VersionScheme.Always,
            javaOptions ++= Seq(
                "-Xmx4G",
                "-XX:+UseG1GC",
                "-Dlog4j.configurationFile=log4j.properties"
            ),
            Compile / mainClass := Some("com.stream.spark.ConsumerRunner"),
            assembly / mainClass := Some("com.stream.spark.ConsumerRunner"),
            assembly / assemblyMergeStrategy := {
                case PathList("META-INF", _*) => MergeStrategy.discard
                case _ => MergeStrategy.first
            }
        )
