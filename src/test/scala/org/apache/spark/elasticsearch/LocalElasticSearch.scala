package org.apache.spark.elasticsearch

import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.{NodeBuilder, Node}

class LocalElasticSearch(val clusterName: String = UUID.randomUUID().toString) {
  lazy val node = buildNode()
  lazy val client = node.client()
  val dataDir = Files.createTempDirectory("elasticsearch").toFile

  private var started = false

  def buildNode(): Node = {
    val settings = Settings.settingsBuilder()
      .put("path.home", dataDir.getAbsolutePath)
      .put("path.logs", s"${dataDir.getAbsolutePath}/logs")
      .put("path.data", s"${dataDir.getAbsolutePath}/data")
      .put("index.store.fs.memory.enabled", true)
      .put("index.number_of_shards", 1)
      .put("index.number_of_replicas", 0)
      .put("cluster.name", clusterName)
      .build()

    val instance = NodeBuilder.nodeBuilder().settings(settings).node()

    started = true

    instance
  }

  def close(): Unit = {
    if (started) {
      client.close()
      node.close()
    }

    try {
      FileUtils.forceDelete(dataDir)
    } catch {
      case e: Exception =>
    }
  }
}
