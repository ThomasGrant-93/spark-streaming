ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

ThisBuild / organization := "com.stream.spark"

val sparkVersion = "3.5.3"

lazy val commonSettings = Seq(
    libraryDependencies ++= Seq(
        "com.typesafe" % "config" % "1.4.3",
        "org.apache.kafka" % "kafka-clients" % "3.8.0",
        "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
        "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
        "com.github.jnr" % "jnr-posix" % "3.1.19"
    ),
    libraryDependencySchemes += "com.github.luben" % "zstd-jni" % VersionScheme.Always,
    Compile / mainClass := Some("com.stream.spark.ConsumerRunner"),
    assembly / mainClass := Some("com.stream.spark.ConsumerRunner"),
    assembly / assemblyMergeStrategy := {
        case PathList("META-INF", _*) => MergeStrategy.discard
        case _ => MergeStrategy.first
    }
)

lazy val local = project
        .in(file("."))
        .enablePlugins(AssemblyPlugin)
        .settings(
            name := "spark-streaming-local",
            commonSettings,
            libraryDependencies ++= Seq(
                "org.apache.spark" %% "spark-core" % sparkVersion,
                "org.apache.spark" %% "spark-sql" % sparkVersion,
                "log4j" % "log4j" % "1.2.17"
            ),
            javaOptions ++= Seq(
                "-Xmx2G",
                "-XX:+UseG1GC",
                "-Dio.netty.tryReflectionSetAccessible=true"
            )
        )

lazy val cluster = project
        .in(file("cluster"))
        .enablePlugins(AssemblyPlugin)
        .settings(
            name := "spark-streaming-cluster",
            commonSettings,
            libraryDependencies ++= Seq(
                "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
                "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
            ),
            javaOptions ++= Seq(
                "-Xmx4G",
                "-XX:+UseG1GC"
            )
        )
