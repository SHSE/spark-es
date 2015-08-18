package org.apache.spark.elasticsearch

import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, LocalTransportAddress, TransportAddress}
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

    val client = getESClientByAddresses(List(partition.node), clusterName)

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

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val endpoint = split.asInstanceOf[ElasticSearchPartition].node

    endpoint match {
      case SocketEndpoint(address, _) => Seq(address)
      case _ => Seq.empty
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val client = getESClient(nodes, clusterName)

    try {
      var partitionsByHost = Map.empty[String, Int]

      def selectNode(nodes: Array[DiscoveryNode]): DiscoveryNode = {
        val (selectedNode, assignedPartitionCount) = nodes
          .map(node => node -> partitionsByHost.getOrElse(node.getId, 0))
          .sortBy { case (node, count) => count }
          .head

        partitionsByHost += selectedNode.getId -> (assignedPartitionCount + 1)

        selectedNode
      }

      val metadata = client.admin().cluster().prepareSearchShards(indexNames: _*).get()

      val nodes = metadata.getNodes.map(node => node.getId -> node).toMap

      val partitions = metadata.getGroups
        .flatMap(group => group.getShards.map(group.getIndex -> _))
        .map { case (index, shard) => (index, shard.getId) -> nodes(shard.currentNodeId) }
        .groupBy { case (indexAndShard, _) => indexAndShard }
        .mapValues(_.map(_._2))
        .mapValues(selectNode)
        .iterator
        .zipWithIndex
        .map { case (((indexName, shardId), node), index) =>
          new ElasticSearchPartition(id, index, indexName, transportAddressToEndpoint(node.getAddress), shardId)
        }
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
    val endpoints = nodes.map(_.split(':')).map {
      case Array(host, port) => SocketEndpoint(host, port.toInt)
      case Array(host) => SocketEndpoint(host, 9300)
    }

    getESClientByAddresses(endpoints, clusterName)
  }

  def endpointToTransportAddress(endpoint: Endpoint): TransportAddress = endpoint match {
    case LocalEndpoint(id) => new LocalTransportAddress(id)
    case SocketEndpoint(address, port) => new InetSocketTransportAddress(address, port)
  }

  def transportAddressToEndpoint(address: TransportAddress): Endpoint = address match {
    case socket: InetSocketTransportAddress =>
      SocketEndpoint(socket.address().getHostName, socket.address().getPort)

    case local: LocalTransportAddress => LocalEndpoint(local.id())

    case _ => throw new RuntimeException("Unsupported transport address")
  }

  def getESClientByAddresses(endpoints: Seq[Endpoint], clusterName: String): TransportClient = {
    val settings = Map("cluster.name" -> clusterName)
    val esSettings = ImmutableSettings.settingsBuilder().put(settings.asJava).build()
    val client = new TransportClient(esSettings)

    val addresses = endpoints.map(endpointToTransportAddress)

    client.addTransportAddresses(addresses: _*)

    client
  }

  class DocumentIterator(var scrollId: String, client: Client, scrollDuration: TimeValue) extends Iterator[ESDocument] {
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

        scrollId = response.getScrollId

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

  sealed abstract class Endpoint

  case class SocketEndpoint(address: String, port: Int) extends Endpoint

  case class LocalEndpoint(id: String) extends Endpoint

  class ElasticSearchPartition(
    rddId: Int,
    override val index: Int,
    val indexName: String,
    val node: Endpoint,
    val shardId: Int) extends Partition {
    override def hashCode(): Int = 41 * (41 + rddId) + index

    override def toString = s"ElasticSearchPartition($index, $indexName, $node, $shardId)"
  }

}


