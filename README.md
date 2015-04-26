# Spark ↔ ElasticSearch
ElasticSearch integration for Apache Spark.

## Features

* Transport client
* Partition per shard
* Data co-location
* Flexible and simple API

## Usage

Read from ElasticSearch using `query_string` query:

```Scala
val query = "name:john"

val documents = sparkContext.esRDD(Seq("localhost"), "cluster1", Seq("index1"), Seq("type1"), query)
```

Read from ElasticSearch using `org.elasticsearch.action.search.SearchRequestBuilder`:

```Scala
val query = QueryBuilders.termQuery("name", "john")

val documents = 
  sparkContext.esRDD(Seq("localhost"), "SparkES", Seq("index1"), Seq("type1"), _.setQuery(query))
```

Save to ElasticSearch:

```Scala
val documents = Seq(
  ESDocument(ESMetadata("1", "type1", "index1"), """{"name": "Sergey Shumov"}"""),
  ESDocument(ESMetadata("2", "type1", "index1"), """{"name": "John Smith"}""")
)

val options = SaveOptions(
  saveOperation = SaveOperation.Create, // Do not overwrite existing documents
  ignoreConflicts = true // Do not fail if document already exists
)

sparkContext
  .parallelize(documents, 2)
  .saveToES(Seq("localhost"), "cluster1", options)
```

Delete from ElasticSearch:

```Scala

// Using metadata RDD
val metadata = Seq(
  ESMetadata("1", "type1", "index1"),
  ESMetadata("2", "type1", "index1")
)

sparkContext
  .parallelize(documents, 2)
  .deleteFromES(Seq("localhost"), "cluster1")

// Using document indices
val ids = Seq("1", "2")

sparkContext
  .parallelize(ids, 2)
  .deleteFromES(Seq("localhost"), "cluster1", "index1", "type1")
```

Custom bulk action:

```Scala
val items = Seq("type1" -> "1", "type2" -> "2")

def handleResponse(response: BulkItemResponse): Unit =
  if (response.isFailed)
    println(response.getFailure.getStatus)

def handleDocument(client: Client, bulk: BulkRequestBuilder, document: (String, String): Unit =
  bulk.add(client.prepareDelete("index1", document._1, document._2))

sparkContext
  .parallelize(items, 2)
  .bulkToES(Seq("localhost"), "cluster1", handleDocument, handleResponse)
```
