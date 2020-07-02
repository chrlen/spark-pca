import Dependencies._

ThisBuild / scalaVersion := "2.11.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organizationName := "spark_pca"

lazy val root = (project in file("."))
  .settings(
    name := "pca",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.1",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.0",
  )


// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
