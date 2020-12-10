package dynamodb

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.core.config.{AwsConfig, CommonAwsConfig}
import io.github.vigoo.zioaws.dynamodb._
import io.github.vigoo.zioaws.{dynamodb, netty}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.{Has, ZLayer}

import java.net.URI
import scala.language.implicitConversions

object AwsLayers {
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
  val ddbLayer: ZLayer[Any, Nothing, DynamoDb] = (awsConfigLayer >>> dynamodb.live).orDie
  val ddbLayer2: ZLayer[Any, Nothing, Has[DynamoDb2.Service]] = (awsConfigLayer >>> DynamoDb2.live).orDie

}
