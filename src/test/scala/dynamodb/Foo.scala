package dynamodb

import java.net.URI

import io.github.vigoo.zioaws.core.config.{AwsConfig, CommonAwsConfig, default}
import io.github.vigoo.zioaws.core.{AwsError, config}
import io.github.vigoo.zioaws.dynamodb.model._
import io.github.vigoo.zioaws.dynamodb.{model, _}
import io.github.vigoo.zioaws.{dynamodb, netty}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.{App, ExitCode, URIO, ZIO, ZLayer}

import scala.language.implicitConversions

object Foo extends App {

  val httpClientLayer = netty.default

  val awsConfigLayer: ZLayer[Any, Throwable, AwsConfig] = httpClientLayer >>> default

  val commonAwsConfigLayer = ZLayer.succeed(
    CommonAwsConfig(
      credentialsProvider = creds,
      region = Some(Region.US_EAST_1),
      endpointOverride = Some(URI.create("http://localhost:8000")),
      commonClientConfig = None
    )
  )

  val awsConfigLayer2: ZLayer[Any, Throwable, AwsConfig] = httpClientLayer ++ commonAwsConfigLayer >>> config
    .configured()

  val ddbLayer: ZLayer[Any, Throwable, DynamoDb] = awsConfigLayer2 >>> dynamodb.live

  val hashKeyName = "id"

  val createTableRequest: CreateTableRequest = CreateTableRequest(
    attributeDefinitions = Seq(
      AttributeDefinition(hashKeyName, ScalarAttributeType.S),
      AttributeDefinition("entitlement", ScalarAttributeType.S)
//      AttributeDefinition("orderDate", ScalarAttributeType.S)
    ),
    tableName = "Entitlement",
    keySchema = Seq(model.KeySchemaElement("id", KeyType.HASH), model.KeySchemaElement("entitlement", KeyType.RANGE)),
    provisionedThroughput = Some(ProvisionedThroughput(1000L, 1500L))
  )

  val program: ZIO[DynamoDb, AwsError, CreateTableResponse.ReadOnly] = for {
    result <- createTable(createTableRequest)
  } yield result

  val createTableProgram: ZIO[Any, Object, CreateTableResponse.ReadOnly] = program.provideLayer(ddbLayer)

  val accessKey: String = "dummy-key"
  val secretAccessKey: String = "dummy-key"
  val creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretAccessKey))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideLayer(ddbLayer).exitCode
}
