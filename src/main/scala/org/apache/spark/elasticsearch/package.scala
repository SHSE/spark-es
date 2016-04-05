package org.apache.spark

import org.apache.spark.rdd.RDD
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkRequestBuilder}
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.index.VersionType

package object elasticsearch {

  implicit class SparkExtensions(sparkContext: SparkContext) {
    def esRDD(
      nodes: Seq[String],
      clusterName: String,
      indexNames: Seq[String],
      typeNames: Seq[String],
      configure: SearchRequestBuilder => Unit = _ => ()): ElasticSearchRDD =
      new ElasticSearchRDD(sparkContext, nodes, clusterName, indexNames, typeNames, configure)

    def esRDD(
      nodes: Seq[String],
      clusterName: String,
      indexNames: Seq[String],
      typeNames: Seq[String],
      query: String): ElasticSearchRDD = {
      SparkOperations.esRDD(sparkContext, nodes, clusterName, indexNames, typeNames, query)
    }
  }

  implicit class RDDExtensions[T](rdd: RDD[T]) {
    def bulkToES(
      nodes: Seq[String],
      clusterName: String,
      handleDocument: (Client, BulkRequestBuilder, T) => Unit,
      handleResponse: ResponseHandler = IgnoreFailure,
      batchSize: Int = 20,
      refreshIndices: Boolean = true): Unit =
      RDDOperations.bulkToES(rdd, nodes, clusterName, handleDocument, handleResponse, batchSize, refreshIndices)
  }

  implicit class DocumentRDDExtensions(rdd: RDD[ESDocument]) {
    def saveToES(
      nodes: Seq[String],
      clusterName: String,
      options: SaveOptions = SaveOptions()): Unit =
      RDDOperations.saveToES(rdd, nodes, clusterName, options)

    def deleteFromES(
      nodes: Seq[String],
      clusterName: String,
      options: SaveOptions = SaveOptions()): Unit =
      RDDOperations.saveToES(rdd, nodes, clusterName, options)
  }

  implicit class PairRDDExtensions(rdd: RDD[(String, String)]) {
    def saveToES(
      nodes: Seq[String],
      clusterName: String,
      indexName: String,
      typeName: String,
      options: SaveOptions = SaveOptions()): Unit =
      RDDOperations.saveToES(rdd, nodes, clusterName, indexName, typeName, options)
  }

  implicit class IndicesRDDExtensions(rdd: RDD[String]) {
    def deleteFromES(
      nodes: Seq[String],
      clusterName: String,
      indexName: String,
      typeName: String,
      options: DeleteOptions = DeleteOptions()): Unit = {
      RDDOperations.deleteFromES(rdd, nodes, clusterName, indexName, typeName, options)
    }
  }

  implicit class MetadataRDDExtensions(rdd: RDD[ESMetadata]) {
    def deleteFromES(
      nodes: Seq[String],
      clusterName: String,
      options: DeleteOptions = DeleteOptions()): Unit =
      RDDOperations.deleteFromES(rdd, nodes, clusterName, options)
  }

  case class SaveOptions(
    batchSize: Int = 20,
    useOptimisticLocking: Boolean = false,
    ignoreConflicts: Boolean = false,
    saveOperation: SaveOperation.SaveOperation = SaveOperation.Index,
    refreshAfterSave: Boolean = true,
    versionType: Option[VersionType] = None)

  case class DeleteOptions(
    batchSize: Int = 20,
    useOptimisticLocking: Boolean = false,
    ignoreMissing: Boolean = false,
    refreshAfterDelete: Boolean = true,
    versionType: Option[VersionType] = None)

  case class ElasticSearchBulkFailedException(response: BulkItemResponse)
    extends RuntimeException("Failed to process bulk request:\n" + response.getFailureMessage)

  case class ESDocument(
    metadata: ESMetadata,
    source: String)

  case class ESMetadata(
    id: String,
    typeName: String,
    indexName: String,
    routing: Option[String] = None,
    version: Option[Long] = None,
    parent: Option[String] = None,
    timestamp: Option[String] = None,
    fields: Option[Map[String, Array[AnyRef]]] = None)

  case class ElasticSearchResult(
    document: ESDocument,
    matchedQueries: Seq[String],
    innerHits: Map[String, ESDocument],
    nodeId: String,
    shardId: Int)

  object SaveOperation extends Enumeration {
    type SaveOperation = Value
    val Create, Index, Update = Value
  }

}



