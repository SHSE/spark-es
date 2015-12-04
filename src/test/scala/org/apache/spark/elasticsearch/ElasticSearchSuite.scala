package org.apache.spark.elasticsearch

import org.scalatest.{BeforeAndAfterEach, Suite}

trait ElasticSearchSuite extends Suite with BeforeAndAfterEach {
  val templateUrls = Seq.empty[String]
  val indexMappings = Map.empty[String, String]

  private var localES: LocalElasticSearch = null

  def es = localES

  override protected def afterEach(): Unit = {
    super.afterEach()

    clean()
  }

  def clean(): Unit = {
    localES.close()
    localES = null
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    localES = new LocalElasticSearch
  }
}


