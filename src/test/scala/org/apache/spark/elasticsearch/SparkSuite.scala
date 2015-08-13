package org.apache.spark.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, Suite}

trait SparkSuite extends Suite with BeforeAndAfterEach {
  private var currentSparkContext: SparkContext = null

  def sparkContext = currentSparkContext

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("self.getClass.getName")


  override protected def beforeEach(): Unit = {
    super.beforeEach()

    currentSparkContext = new SparkContext(conf)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()

    currentSparkContext.stop()

    System.clearProperty("spark.master.port")
  }
}
