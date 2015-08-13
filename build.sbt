name := "spark-es"

organization := "com.github.shse"

version := "1.0.3"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

javaOptions += "-Xmx2G"

parallelExecution in Test := false