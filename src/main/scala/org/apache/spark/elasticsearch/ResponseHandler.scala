package org.apache.spark.elasticsearch

import org.elasticsearch.action.bulk.BulkItemResponse

sealed abstract class ResponseHandler

case object IgnoreFailure extends ResponseHandler

case object ThrowExceptionOnFailure extends ResponseHandler

case class CustomHandler(handler: BulkItemResponse => Unit) extends ResponseHandler
