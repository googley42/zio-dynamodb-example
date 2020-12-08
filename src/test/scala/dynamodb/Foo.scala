package dynamodb

import java.net.URI

import io.github.vigoo.zioaws.core.aspects.AwsCallAspect
import io.github.vigoo.zioaws.core.config.{AwsConfig, CommonAwsConfig}
import io.github.vigoo.zioaws.core.{config, AwsError, AwsServiceBase}
import io.github.vigoo.zioaws.dynamodb.model._
import io.github.vigoo.zioaws.dynamodb.model.primitives.AttributeName
import io.github.vigoo.zioaws.dynamodb.{model, _}
import io.github.vigoo.zioaws.{dynamodb, netty}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbAsyncClientBuilder}
import zio.{App, ExitCode, Has, IO, URIO, ZIO, ZLayer, ZManaged}

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
    lastEvaluatedKey: Map[AttributeName, AttributeValue]
  ): QueryRequest =
    QueryRequest(
      tableName = "Entitlement",
      indexName = Some("idOrderDate"),
      keyConditionExpression = Some("id = :id AND orderDate >= :orderDate"),
      select = Some(Select.ALL_ATTRIBUTES),
      expressionAttributeValues =
        Some(Map(hashKeyName -> AttributeValue(s = Some(id)), "orderDate" -> AttributeValue(s = Some(DateCutOff)))),
      limit = Some(limit),
      exclusiveStartKey = Some(lastEvaluatedKey)
    )

  val program = for {
    _ <- createTable(createTableRequest)
    putItemResults <- ZIO.foreach((1 to 5)) { i =>
      putItem(putItemRequest(i))
    }
  } yield putItemResults

  val createTableProgram = program.provideLayer(ddbLayer)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideLayer(ddbLayer).exitCode

  // can use AwsCallAspect.identity

  val ddbLayer2: ZLayer[Any, Throwable, DynamoDb2.DynamoDb2] = awsConfigLayer >>> DynamoDb2.live2

}
