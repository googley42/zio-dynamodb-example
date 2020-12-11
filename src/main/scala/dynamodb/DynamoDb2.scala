package dynamodb

import io.github.vigoo.zioaws.core.aspects.AwsCallAspect
import io.github.vigoo.zioaws.core.config.AwsConfig
import io.github.vigoo.zioaws.core.{AwsError, AwsServiceBase}
import io.github.vigoo.zioaws.dynamodb.model.primitives.AttributeName
import io.github.vigoo.zioaws.dynamodb.model.{AttributeValue, QueryRequest, QueryResponse}
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbAsyncClientBuilder}
import zio.stream.{Stream, ZStream}
import zio.{Has, IO, ZIO, ZLayer, ZManaged}

// Totally hacked version of `DynamoDb` module from zio-aws to demonstrate query streaming based on server side paging
object DynamoDb2 {
  type DynamoDb2 = Has[Service]

  trait Service {
    def queryUnpaged(request: QueryRequest): IO[AwsError, QueryResponse.ReadOnly]
    def query2(queryRequest: QueryRequest, limit: Int): ZStream[Any, AwsError, Map[AttributeName, AttributeValue]]
  }

  val live: ZLayer[AwsConfig, Throwable, Has[Service]] = managed2(identity).toLayer

  def managed2(
    customization: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder
  ): ZManaged[AwsConfig, Throwable, DynamoDb2.Service] =
    for {
      awsConfig <- ZManaged.service[AwsConfig.Service]
      b0 <- awsConfig
        .configure[DynamoDbAsyncClient, DynamoDbAsyncClientBuilder](DynamoDbAsyncClient.builder())
        .toManaged_
      b1 <- awsConfig.configureHttpClient[DynamoDbAsyncClient, DynamoDbAsyncClientBuilder](b0).toManaged_
      client <- ZIO(customization(b1).build()).toManaged_
    } yield new DynamoDbImpl2(client, AwsCallAspect.identity, ().asInstanceOf[Any])

  def queryUnpaged(request: QueryRequest): ZIO[DynamoDb2, AwsError, QueryResponse.ReadOnly] =
    ZIO.accessM(_.get.queryUnpaged(request))

  def query2(request: QueryRequest, limit: Int = 10): ZStream[DynamoDb2, AwsError, Map[AttributeName, AttributeValue]] =
    for {
      x <- ZStream.service[DynamoDb2.Service]
      response <- x.query2(request, limit)
    } yield response
}

class DynamoDbImpl2[R](val api: DynamoDbAsyncClient, val aspect: AwsCallAspect[R], r: R)
    extends DynamoDb2.Service
    with AwsServiceBase[R, DynamoDbImpl2] {
  override val serviceName: String = "shockinglyButcheredDynamoDbService"

  val PageLimit = 10 // TODO: this could be part of config or a defaulted param

  // returns QueryResponse directly without any streaming/paging - used by query below
  def queryUnpaged(request: QueryRequest): IO[AwsError, QueryResponse.ReadOnly] =
    asyncRequestResponse[
      software.amazon.awssdk.services.dynamodb.model.QueryRequest,
      software.amazon.awssdk.services.dynamodb.model.QueryResponse
    ]("queryUnpaged", api.query)(request.buildAwsValue()).map(QueryResponse.wrap).provide(r)

  /**
    * Uses the server siding paging API of DynamoDb rather than the reactive-streams based DynamoDb api and combines
    * this ZStream unfoldM to produce a ZIO stream
    *
    * @param queryRequest
    * @param limit page size
    * @return A ZStream of Map[AttributeName, AttributeValue]]
    */
  def query2(
    queryRequest: QueryRequest,
    limit: Int = 10
  ): ZStream[Any, AwsError, Map[AttributeName, AttributeValue]] = {
    val emptyLek = Option.empty[Map[AttributeName, AttributeValue]]
    val stream =
      Stream.unfoldM(emptyLek) { lek =>
        val lekNormalised: Option[Map[AttributeName, AttributeValue]] = normaliseLek(lek)
        val q = queryRequest.copy(limit = Some(limit), exclusiveStartKey = lekNormalised)
        val effect =
          queryUnpaged(q).map(_.editable)
        effect
          .map { qr =>
            Some((qr, qr.lastEvaluatedKey))
          }
      }

    stream
      .takeUntil { qr =>
        qr.items.fold(0)(_.size) == 0 || normaliseLek(qr.lastEvaluatedKey).isEmpty
      }
      .flatMap(
        qr => Stream.fromIterable(qr.items.fold(Iterable.empty[Map[AttributeName, AttributeValue]])(it => it))
      )
  }

  /*
  Needed as I was getting 400 error for empty Lists and Maps in LEK map attributes
  BEFORE
  Some(Map(orderDate -> AttributeValue(Some(orderDate4),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None), entitlement -> AttributeValue(Some(entitlement4),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None), id -> AttributeValue(Some(id),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None)))
  AFTER
  Some(Map(orderDate -> AttributeValue(Some(orderDate4),None,None,None,None,None,None,None,None,None), entitlement -> AttributeValue(Some(entitlement4),None,None,None,None,None,None,None,None,None), id -> AttributeValue(Some(id),None,None,None,None,None,None,None,None,None)))
   */
  private def normaliseLek(
    lek: Option[Map[AttributeName, AttributeValue]]
  ): Option[Map[AttributeName, AttributeValue]] = {
    def normaliseList[T](o: Option[Iterable[T]]) =
      o.fold[Option[Iterable[T]]](None)(it => if (it.isEmpty) None else Some(it))
    def normaliseMap[K, V](o: Option[Map[K, V]]) =
      o.fold[Option[Map[K, V]]](None)(map => if (map.isEmpty) None else Some(map))

    lek.fold[Option[Map[AttributeName, AttributeValue]]](None) { map =>
      if (map.isEmpty)
        None
      else
        Some(map.mapValues { av =>
          AttributeValue(
            av.s,
            av.n,
            av.b,
            normaliseList(av.ss),
            normaliseList(av.ns),
            normaliseList(av.bs),
            normaliseMap(av.m),
            normaliseList(av.l),
            av.nul,
            av.bool
          )
        })
    }
  }

}
