package org.apache.spark.elasticsearch

import org.apache.spark.rdd.RDD
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkRequestBuilder}
import org.elasticsearch.client.Client
import org.elasticsearch.rest.RestStatus

object RDDOperations {
  def deleteFromES(
    rdd: RDD[ESMetadata],
    nodes: Seq[String],
    clusterName: String,
    options: DeleteOptions): Unit = {
    def handleResponse(response: BulkItemResponse): Unit = {
      if (response.isFailed && (response.getFailure.getStatus != RestStatus.NOT_FOUND || !options.ignoreMissing))
        throw new ElasticSearchBulkFailedException(response)
    }

    def handleDocument(client: Client, bulk: BulkRequestBuilder, document: ESMetadata): Unit = {
      val request = client.prepareDelete(
        document.indexName,
        document.typeName,
        document.id
      )

      document.parent.foreach(request.setParent)
      document.routing.foreach(request.setRouting)

      if (options.useOptimisticLocking) {
        document.version.foreach(request.setVersion)
        options.versionType.foreach(request.setVersionType)
      }

      bulk.add(request)
    }

    bulkToES[ESMetadata](
      rdd, nodes, clusterName, handleDocument, CustomHandler(handleResponse), options.batchSize, options.refreshAfterDelete
    )
  }

  def deleteFromES(
    rdd: RDD[String],
    nodes: Seq[String],
    clusterName: String,
    indexName: String,
    typeName: String,
    options: DeleteOptions): Unit = {
    deleteFromES(rdd.map(ESMetadata(_, typeName, indexName)), nodes, clusterName, options)
  }

  def saveToES(
    rdd: RDD[(String,String)],
    nodes: Seq[String],
    clusterName: String,
    indexName: String,
    typeName: String,
    options: SaveOptions): Unit = {
    val documents = rdd.map { case (id, source) =>
      ESDocument(ESMetadata(id, typeName, indexName), source)
    }

    saveToES(documents, nodes, clusterName, options)
  }

  def bulkToES[T](
    rdd: RDD[T],
    nodes: Seq[String],
    clusterName: String,
    handleDocument: (Client, BulkRequestBuilder, T) => Unit,
    handleResponse: ResponseHandler = IgnoreFailure,
    batchSize: Int,
    refreshIndices: Boolean): Unit = {

    val indices = rdd.context.accumulableCollection(scala.collection.mutable.TreeSet.empty[String])

    rdd.foreachPartition { partition =>
      val client = ElasticSearchRDD.getESClient(nodes, clusterName)

      try {
        for (batch <- partition.grouped(batchSize)) {
          val bulk = client.prepareBulk()

          for (document <- batch) {
            handleDocument(client, bulk, document)
          }

          if (bulk.numberOfActions() > 0) {
            val response = bulk.get()

            for (item <- response.getItems) {
              if (refreshIndices)
                indices += item.getIndex

              handleResponse match {
                case IgnoreFailure =>
                case ThrowExceptionOnFailure => throw new ElasticSearchBulkFailedException(item)
                case CustomHandler(handler) => handler(item)
              }
            }
          }
        }
      } finally {
        client.close()
      }
    }

    if (refreshIndices)
      refresh(nodes, clusterName, indices.value.toSeq)
  }

  private[elasticsearch] def refresh(nodes: Seq[String], clusterName: String, indexNames: Seq[String]): Unit = {
    val client = ElasticSearchRDD.getESClient(nodes, clusterName)

    try {
      client.admin().indices().prepareRefresh(indexNames: _*).get()
    } finally {
      client.close()
    }
  }

  def saveToES(
    rdd: RDD[ESDocument],
    nodes: Seq[String],
    clusterName: String,
    options: SaveOptions): Unit = {
    def handleResponse(response: BulkItemResponse): Unit = {
      if (response.isFailed && (response.getFailure.getStatus != RestStatus.CONFLICT || !options.ignoreConflicts))
        throw new ElasticSearchBulkFailedException(response)
    }

    def handleDocument(client: Client, bulk: BulkRequestBuilder, document: ESDocument): Unit = {
      options.saveOperation match {
        case SaveOperation.Index | SaveOperation.Create =>
          val request = client.prepareIndex(
            document.metadata.indexName,
            document.metadata.typeName,
            document.metadata.id)

          request.setSource(document.source)

          if (options.saveOperation == SaveOperation.Create)
            request.setCreate(true)

          document.metadata.parent.foreach(request.setParent)
          document.metadata.routing.foreach(request.setRouting)
          document.metadata.timestamp.foreach(request.setTimestamp)

          if (options.useOptimisticLocking) {
            document.metadata.version.foreach(request.setVersion)
            options.versionType.foreach(request.setVersionType)
          }

          bulk.add(request)

        case SaveOperation.Update =>
          val request = client.prepareUpdate(
            document.metadata.indexName,
            document.metadata.typeName,
            document.metadata.id)

          request.setDoc(document.source)

          document.metadata.parent.foreach(request.setParent)
          document.metadata.routing.foreach(request.setRouting)

          bulk.add(request)
      }
    }

    bulkToES[ESDocument](
      rdd, nodes, clusterName, handleDocument, CustomHandler(handleResponse), options.batchSize, options.refreshAfterSave
    )
  }
}
