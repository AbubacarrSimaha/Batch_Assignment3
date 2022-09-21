name := "Week3"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"
lazy val root = (project in file("."))
  .settings(
    name := "Scala_Week_1",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.3.0",
      "org.apache.spark" %% "spark-avro" % "3.3.0",
      "org.apache.spark" %% "spark-core" % "3.3.0",
      "org.apache.spark" %% "spark-avro" % "3.2.1"
    )
  )