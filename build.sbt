name := "spark-es"

organization := "com.github.shse"

version := "1.0.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.5.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

javaOptions += "-Xmx2G"

parallelExecution in Test := false