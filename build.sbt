ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "FinalProject_Spark"
  )

//libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.18.0"
// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.2"
// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.17.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.2"

// https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.39.2.0"
