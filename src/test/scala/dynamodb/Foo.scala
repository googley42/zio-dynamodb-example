package dynamodb

import java.net.URI

import io.github.vigoo.zioaws.core.{config, AwsError}
import io.github.vigoo.zioaws.core.config.{AwsConfig, CommonAwsConfig}
import io.github.vigoo.zioaws.dynamodb.model._
import io.github.vigoo.zioaws.dynamodb.model.primitives.AttributeName
import io.github.vigoo.zioaws.dynamodb.{model, _}
import io.github.vigoo.zioaws.{dynamodb, netty}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.stream.{Stream, ZStream}
import zio.{stream, App, Chunk, ExitCode, UIO, URIO, ZIO, ZLayer}

import scala.language.implicitConversions

object Foo extends App {
  val accessKey: String = "dummy-key"
  val secretAccessKey: String = "dummy-key"
  val creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretAccessKey))

  val httpClientLayer = netty.default

  val commonAwsConfigLayer = ZLayer.succeed(
    CommonAwsConfig(
      credentialsProvider = creds,
      region = Some(Region.US_EAST_1),
      endpointOverride = Some(URI.create("http://localhost:8000")),
      commonClientConfig = None
    )
  )

  val awsConfigLayer: ZLayer[Any, Throwable, AwsConfig] = httpClientLayer ++ commonAwsConfigLayer >>> config
    .configured()
  val ddbLayer: ZLayer[Any, Throwable, DynamoDb] = awsConfigLayer >>> dynamodb.live
  val ddbLayer2: ZLayer[Any, Throwable, DynamoDb2.DynamoDb2] = awsConfigLayer >>> DynamoDb2.live2
  val hashKeyName = "id"
  val DateCutOff: String = "orderDate0"

  val createTableRequest: CreateTableRequest = CreateTableRequest(
    attributeDefinitions = Seq(
      AttributeDefinition(hashKeyName, ScalarAttributeType.S),
      AttributeDefinition("entitlement", ScalarAttributeType.S),
      AttributeDefinition("orderDate", ScalarAttributeType.S)
    ),
    tableName = "Entitlement",
    keySchema =
      Seq(model.KeySchemaElement(hashKeyName, KeyType.HASH), model.KeySchemaElement("entitlement", KeyType.RANGE)),
    localSecondaryIndexes = Some(
      Seq(
        LocalSecondaryIndex(
          "idOrderDate",
          Seq(KeySchemaElement(hashKeyName, KeyType.HASH), KeySchemaElement("orderDate", KeyType.RANGE)),
          projection = Projection(projectionType = Some(ProjectionType.ALL))
        )
      )
    ),
    provisionedThroughput = Some(ProvisionedThroughput(5L, 5L))
  )

  def putItemRequest(i: Int) =
    PutItemRequest(
      tableName = "Entitlement",
      item = Map(
        "id" -> AttributeValue(s = Some("id")),
        "entitlement" -> AttributeValue(s = Some(s"entitlement$i")),
        "orderDate" -> AttributeValue(s = Some(s"orderDate$i")),
        "accountId" -> AttributeValue(s = Some(s"1234abcd"))
      )
    )

  def findAllByIdInTheLastYear(
    limit: Int,
    id: String,
    lastEvaluatedKey: Option[Map[AttributeName, AttributeValue]]
  ): QueryRequest =
    QueryRequest(
      tableName = "Entitlement",
      indexName = Some("idOrderDate"),
      keyConditionExpression = Some("id = :id AND orderDate >= :orderDate"),
      select = Some(Select.ALL_ATTRIBUTES),
      expressionAttributeValues =
        Some(Map(":id" -> AttributeValue(s = Some(id)), ":orderDate" -> AttributeValue(s = Some(DateCutOff)))),
      limit = Some(limit),
      exclusiveStartKey = lastEvaluatedKey
    )

  def lekStart: Option[Map[AttributeName, AttributeValue]] = QueryResponse().lastEvaluatedKey

  /*
exclusiveStartKey = {Some@7642} "Some(Map(orderDate -> AttributeValue(Some(orderDate2),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None), entitlement -> AttributeValue(Some(entitlement2),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None), id -> AttributeValue(Some(id),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None)))"
 value = {Map$Map3@7724} "Map$Map3" size = 3
  0 = {Tuple2@9510} "(orderDate,AttributeValue(Some(orderDate2),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None))"
  1 = {Tuple2@9511} "(entitlement,AttributeValue(Some(entitlement2),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None))"
  2 = {Tuple2@9512} "(id,AttributeValue(Some(id),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None))"
   */
  def lekStartRow3: Option[Map[AttributeName, AttributeValue]] = Some(
    Map(
      "orderDate" -> AttributeValue(s = Some("orderDate2")),
      "entitlement" -> AttributeValue(s = Some("entitlement2")),
      "id" -> AttributeValue(s = Some("id"))
    )
  )

  /*
  Needed as I was getting 400 error for empty Lists and Maps in LEK map attributes
  BEFORE
  Some(Map(orderDate -> AttributeValue(Some(orderDate4),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None), entitlement -> AttributeValue(Some(entitlement4),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None), id -> AttributeValue(Some(id),None,None,Some(List()),Some(List()),Some(List()),Some(Map()),Some(List()),None,None)))
  AFTER
  Some(Map(orderDate -> AttributeValue(Some(orderDate4),None,None,None,None,None,None,None,None,None), entitlement -> AttributeValue(Some(entitlement4),None,None,None,None,None,None,None,None,None), id -> AttributeValue(Some(id),None,None,None,None,None,None,None,None,None)))
   */
  def normaliseLek(lek: Option[Map[AttributeName, AttributeValue]]) = {
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

  /*
    Mismatch in types
    QueryResult.ReadOnly.lek = lek.readOnly
    QueryResult.lek = lek
   */
  def streamQuery() = {
    val emptyLek = Option.empty[Map[AttributeName, AttributeValue]]
    val stream =
      Stream.unfoldM(emptyLek) { lek =>
        val lekNormalised: Option[Map[AttributeName, AttributeValue]] = normaliseLek(lek)
        println(lek)
        println(lekNormalised)
        println()
        val queryRequest = findAllByIdInTheLastYear(limit = 2, "id", lekNormalised)
        val effect =
          DynamoDb2.query2(queryRequest).map(_.editable)
        effect
          .provideLayer(ddbLayer2)
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

  val program2 = for {
    _ <- createTable(createTableRequest)
    _ <- ZIO.foreach((1 to 5)) { i =>
      putItem(putItemRequest(i))
    }
    response <- streamQuery.runCollect

  } yield response

  val program = for {
    _ <- createTable(createTableRequest)
    _ <- ZIO.foreach((1 to 5)) { i =>
      putItem(putItemRequest(i))
    }
    response <- DynamoDb2.query2(findAllByIdInTheLastYear(limit = 2, id = "id", lekStart))
    items <- response.items.map(xs => xs.map(_.mapValues(av => av.sValue)))

  } yield items

  val rawQueryProgram = program.provideLayer(ddbLayer ++ ddbLayer2)
  val streamedQueryProgram = program2.provideLayer(ddbLayer ++ ddbLayer2)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideLayer(ddbLayer ++ ddbLayer2).exitCode

}
