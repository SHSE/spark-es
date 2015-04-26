package org.apache.spark.elasticsearch

import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.SearchHit

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class ElasticSearchRDD(
  @transient sparkContext: SparkContext,
  nodes: Seq[String],
  clusterName: String,
  indexNames: Seq[String],
  typeNames: Seq[String],
  configureSearchRequest: SearchRequestBuilder => Unit,
  scrollDuration: Duration = Duration(30, TimeUnit.SECONDS))
  extends RDD[ESDocument](sparkContext, Nil) {

  import ElasticSearchRDD._

  override def compute(split: Partition, context: TaskContext): Iterator[ESDocument] = {
    val partition = split.asInstanceOf[ElasticSearchPartition]

    val client = getESClientByAddresses(List(partition.node.address()), clusterName)

    val requestBuilder = client.prepareSearch(indexNames: _*)
      .setTypes(typeNames: _*)
      .setPreference(s"_shards:${partition.shardId};_local")
      .setSearchType(SearchType.SCAN)
      .setScroll(TimeValue.timeValueMillis(scrollDuration.toMillis))

    configureSearchRequest(requestBuilder)

    val scrollId = requestBuilder.get().getScrollId

    TaskContext.get().addTaskCompletionListener { _ =>
      client.prepareClearScroll().addScrollId(scrollId).get()
      client.close()
    }

    new DocumentIterator(scrollId, client, TimeValue.timeValueMillis(scrollDuration.toMillis))
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[ElasticSearchPartition].node.getHostAddress)

  override protected def getPartitions: Array[Partition] = {
    val client = getESClient(nodes, clusterName)

    try {
      val state = client.admin().cluster().prepareState().get().getState

      var partitionsByHost = Map.empty[DiscoveryNode, Int]

      def selectNode(nodes: Seq[DiscoveryNode]): DiscoveryNode = {
        val (selectedNode, assignedPartitionCount) = nodes
          .map(node => node -> partitionsByHost.getOrElse(node, 0))
          .sortBy { case (node, count) => count }
          .head

        partitionsByHost += selectedNode -> (assignedPartitionCount + 1)

        selectedNode
      }

      val partitions = state.getRoutingTable
        .allShards(indexNames: _*)
        .asScala
        .groupBy(_.shardId())
        .mapValues(_.map(_.currentNodeId()))
        .mapValues(nodeIds => nodeIds.map(state.getNodes.get))
        .iterator
        .map { case (shardId, items) => (shardId.getIndex, shardId.getId, selectNode(items)) }
        .zipWithIndex
        .map { case ((indexName, shardId, nodeAddress), index) => new ElasticSearchPartition(id, index, indexName, nodeAddress, shardId) }
        .map(_.asInstanceOf[Partition])
        .toArray

      if (partitions.isEmpty) {
        logWarning("Found no partitions for indices: " + indexNames.mkString(", "))
      } else {
        logInfo(s"Found ${partitions.length} partition(s): " + partitions.mkString(", "))
      }

      partitions
    } finally {
      client.close()
    }
  }
}

object ElasticSearchRDD {
  def getESClient(nodes: Seq[String], clusterName: String): Client = {
    val addresses = nodes.map(_.split(':')).map {
      case Array(host, port) => new InetSocketTransportAddress(host, port.toInt)
      case Array(host) => new InetSocketTransportAddress(host, 9300)
    }

    getESClientByAddresses(addresses, clusterName)
  }

  def getESClientByAddresses(addresses: Seq[TransportAddress], clusterName: String): TransportClient = {
    val settings = Map("cluster.name" -> clusterName)
    val esSettings = ImmutableSettings.settingsBuilder().put(settings.asJava).build()
    val client = new TransportClient(esSettings)

    client.addTransportAddresses(addresses: _*)

    client
  }

  class DocumentIterator(scrollId: String, client: Client, scrollDuration: TimeValue) extends Iterator[ESDocument] {
    private val batch = scala.collection.mutable.Queue.empty[ESDocument]

    def searchHitToDocument(hit: SearchHit): ESDocument = {
      ESDocument(
        ESMetadata(
          hit.getId,
          hit.getType,
          hit.getIndex,
          None,
          Some(hit.getVersion),
          Option(hit.field("_parent")).map(_.getValue[String]),
          Option(hit.field("_timestamp")).map(_.getValue[String])
        ),
        hit.getSourceAsString
      )
    }

    override def hasNext: Boolean = {
      if (batch.nonEmpty)
        true
      else {
        val response = client.prepareSearchScroll(scrollId).setScroll(scrollDuration).get()
        val hits = response.getHits.hits()

        if (hits.isEmpty) {
          false
        } else {
          hits.iterator.foreach(item => batch.enqueue(searchHitToDocument(item)))
          true
        }
      }
    }

    override def next(): ESDocument = batch.dequeue()
  }

  class ElasticSearchPartition(
    rddId: Int,
    override val index: Int,
    val indexName: String,
    val node: DiscoveryNode,
    val shardId: Int) extends Partition {
    override def hashCode(): Int = 41 * (41 + rddId) + index

    override def toString = s"ElasticSearchPartition($index, $indexName, $node, $shardId)"
  }

}


