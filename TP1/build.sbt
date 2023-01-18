ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "DMB_PUBG"
  )
