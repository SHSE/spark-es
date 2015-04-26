package org.apache.spark.elasticsearch

import org.apache.spark.SparkContext
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}

object SparkOperations {
  def esRDD(
    sparkContext: SparkContext,
    nodes: Seq[String],
    clusterName: String,
    indexNames: Seq[String],
    typeNames: Seq[String],
    query: String): ElasticSearchRDD = {
    def setQuery(request: SearchRequestBuilder): Unit = {
      request.setQuery(
        QueryBuilders.constantScoreQuery(
          FilterBuilders.queryFilter(
            QueryBuilders.queryStringQuery(query)
          ).cache(true)
        )
      )
    }

    new ElasticSearchRDD(sparkContext, nodes, clusterName, indexNames, typeNames, setQuery)
  }
}
