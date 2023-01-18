ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "TP1 EMB"
  )
