package org.apache.spark.elasticsearch

import org.elasticsearch.common.settings.ImmutableSettings
import org.scalatest.FunSuite

class Tests extends FunSuite with SparkSuite with ElasticSearchSuite {
  test("Reads documents from multiple shards") {
    val client = es.client

    val indexName = "index-with-multiple-shards"

    client.admin().indices().prepareCreate(indexName)
      .setSettings(ImmutableSettings.settingsBuilder()
        .put("index.number_of_replicas", 0)
        .put("index.number_of_shards", 2)
        .build()
      )
      .get()

    for (i <- 1 to 1000) {
      client.prepareIndex(indexName, "foo", i.toString).setSource("{}").get()
    }

    client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().get()
    client.admin().indices().prepareRefresh(indexName).get()

    val rdd = sparkContext.esRDD(Seq("localhost"), es.clusterName, Seq(indexName), Seq("foo"), "*")

    assert(rdd.partitions.length == 2)
    assert(rdd.collect().map(_.metadata.id).sorted.toList == (1 to 1000).map(_.toString).sorted.toList)
  }

  test("Writes documents to ElasticSearch") {
    val client = es.client

    val indexName = "index1"

    sparkContext.parallelize(Seq(1, 2, 3, 4))
      .map(id => ESDocument(ESMetadata(id.toString, "foo", indexName), "{}"))
      .saveToES(Seq("localhost"), es.clusterName)

    client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().get()
    client.admin().indices().prepareRefresh(indexName).get()

    assert(client.prepareGet(indexName, "foo", "1").get().isExists)
    assert(client.prepareGet(indexName, "foo", "2").get().isExists)
    assert(client.prepareGet(indexName, "foo", "3").get().isExists)
    assert(client.prepareGet(indexName, "foo", "4").get().isExists)
  }
}
