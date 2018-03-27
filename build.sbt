name := "Scala4fun"

version := "0.1"

scalaVersion := "2.11.11"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "datastax" % "spark-cassandra-connector" % "2.0.1-s_2.11"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.1" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.1" % "provided"
//libraryDependencies += "org.apache.kafka" %% "kafka" % "1.0.0"
//libraryDependencies += "com.maxmind.geoip2" % "geoip2" % "2.5.0"

resolvers += "SnowPlow Repo" at "http://maven.snplow.com/releases/"
resolvers += "Twitter Maven Repo" at "http://maven.twttr.com/"
libraryDependencies += "com.snowplowanalytics"  %% "scala-maxmind-iplookups"  % "0.2.0"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.1"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.1"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.1"



