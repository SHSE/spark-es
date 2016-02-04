name := "spark-es"

organization := "com.github.shse"

version := "1.0.7"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"

libraryDependencies += ("org.elasticsearch" % "elasticsearch" % "2.2.0").exclude("joda-time", "joda-time")

libraryDependencies += "joda-time" % "joda-time" % "2.8.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

javaOptions += "-Xmx2G"

parallelExecution in Test := false
