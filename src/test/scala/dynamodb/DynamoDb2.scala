package dynamodb

import io.github.vigoo.zioaws.core.aspects.AwsCallAspect
import io.github.vigoo.zioaws.core.config.AwsConfig
import io.github.vigoo.zioaws.core.{AwsError, AwsServiceBase}
import io.github.vigoo.zioaws.dynamodb.model.{QueryRequest, QueryResponse}
import software.amazon.awssdk.services.dynamodb.{model, DynamoDbAsyncClient, DynamoDbAsyncClientBuilder}
import zio.{Has, IO, ZIO, ZLayer, ZManaged}

object DynamoDb2 {
  type DynamoDb2 = Has[Service]

  trait Service {
    def query2(request: QueryRequest): IO[AwsError, QueryResponse.ReadOnly]
  }

  val live2: ZLayer[AwsConfig, Throwable, Has[Service]] = managed2(identity).toLayer

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

  def query2(request: QueryRequest): ZIO[DynamoDb2, AwsError, QueryResponse.ReadOnly] =
    ZIO.accessM(_.get.query2(request))
}

class DynamoDbImpl2[R](val api: DynamoDbAsyncClient, val aspect: AwsCallAspect[R], r: R)
    extends DynamoDb2.Service
    with AwsServiceBase[R, DynamoDbImpl2] {
  override val serviceName: String = "shockinglyButcheredDynamoDbService"

  def query2(request: QueryRequest): IO[AwsError, QueryResponse.ReadOnly] =
    asyncRequestResponse[
      software.amazon.awssdk.services.dynamodb.model.QueryRequest,
      software.amazon.awssdk.services.dynamodb.model.QueryResponse
    ]("query2", api.query)(request.buildAwsValue()).map(QueryResponse.wrap).provide(r)

}
