import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder
import org.apache.spark.elasticsearch._

object Tryout {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[2]", "SparkES")

    val dataDir = Files.createTempDirectory("elasticsearch").toFile

    dataDir.deleteOnExit()

    val settings = ImmutableSettings.settingsBuilder()
      .put("path.logs", s"${dataDir.getAbsolutePath}/logs")
      .put("path.data", s"${dataDir.getAbsolutePath}/data")
      .put("index.store.type", "memory")
      .put("index.store.fs.memory.enabled", true)
      .put("gateway.type", "none")
      .put("index.number_of_shards", 1)
      .put("index.number_of_replicas", 0)
      .put("cluster.name", "SparkES")
      .build()

    val node = NodeBuilder.nodeBuilder().settings(settings).node()

    val client = node.client()

    sparkContext
      .parallelize(Seq(
      ESDocument(ESMetadata("2", "type1", "index1"), """{"name": "John Smith"}"""),
      ESDocument(ESMetadata("1", "type1", "index1"), """{"name": "Sergey Shumov"}""")
    ), 2)
      .saveToES(Seq("localhost"), "SparkES")
    
    client.admin().cluster().prepareHealth("index1").setWaitForGreenStatus().get()

    val documents = sparkContext.esRDD(
      Seq("localhost"), "SparkES", Seq("index1"), Seq("type1"), "name:sergey")

    println(documents.count())

    documents.foreach(println)

    sparkContext.stop()

    client.close()
    node.stop()
    node.close()

    FileUtils.deleteQuietly(dataDir)
  }
}
