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
import zio.{stream, App, ExitCode, UIO, URIO, ZIO, ZLayer}

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

  /*
    def findAllByIdInTheLastYear(
      dynamoDbClient: DynamoDbAsyncClient,
      limit: Int,
      id: String,
      lastEvaluatedKey: Row
    ): CompletableFuture[QueryResponse] = {
      val qr = QueryRequest
        .builder()
        .select(Select.ALL_ATTRIBUTES)
        .tableName("Entitlement")
        .indexName("idOrderDate")
        .keyConditionExpression("id = :id AND orderDate >= :orderDate")
        .expressionAttributeValues(
          Map(
            ":id" -> AttributeValue.builder().s(id).build(),
            ":orderDate" -> AttributeValue.builder().s(DateCutOff).build()
          ).asJava
        )
        .limit(limit)

      dynamoDbClient.query(
        qr.exclusiveStartKey(lastEvaluatedKey).build
      )
    }
   */

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

  def lekStart = QueryResponse().lastEvaluatedKey

  /*
    val stream: ZStream[Console, Throwable, QueryResponse] = Stream.unfoldM(lekStart) { lek =>
      val task: Task[QueryResponse] = // constant memory space processing here
        ZIO.fromCompletionStage(DbHelper.findAllByIdInTheLastYear(client, limit = 3, "id", lek))
      task
        .map { qr =>
          Some((qr, qr.lastEvaluatedKey()))
        }
    }

    stream
      .tap(_ => console.putStrLn("Stream >>> fetched a batch ot items"))
      .takeUntil { qr =>
        qr.items.size == 0 || qr.lastEvaluatedKey.size == 0
      }
      .flatMap(qr => Stream.fromIterable(qr.items.asScala))
   */

  /*
    Mismatch in types
    QueryResult.ReadOnly.lek = lek.readOnly
    QueryResult.lek = lek
   */
  def s() = {
    val stream: ZStream[DynamoDb2.DynamoDb2, AwsError, QueryResponse] =
      Stream.unfoldM(lekStart) { lek =>
        val effect =
          DynamoDb2.query2(findAllByIdInTheLastYear(limit = 3, "id", lek)).map(_.editable)
        effect
//          .provideLayer(ddbLayer2)
          .map { qr =>
            Some((qr, qr.lastEvaluatedKey))
          }
      }

//    stream
//      .takeUntil { qr =>
//        qr.items.size == 0 || qr.lastEvaluatedKey.size == 0
//      }
//      .flatMap(qr => Stream.fromIterable(qr.items))

  }

  val program = for {
    _ <- createTable(createTableRequest)
    _ <- ZIO.foreach((1 to 5)) { i =>
      putItem(putItemRequest(i))
    }
    response <- DynamoDb2.query2(findAllByIdInTheLastYear(limit = 2, id = "id", lekStart))
    items <- response.items.map(xs => xs.map(_.mapValues(av => av.sValue)))

  } yield items

  val createTableProgram = program.provideLayer(ddbLayer ++ ddbLayer2)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideLayer(ddbLayer ++ ddbLayer2).exitCode

}
