package dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import dynamodb.AwsLayers.ddbLayer
import dynamodb.Requests._
import io.github.vigoo.zioaws.core.AwsError
import io.github.vigoo.zioaws.dynamodb.model.AttributeValue
import io.github.vigoo.zioaws.dynamodb.model.primitives.AttributeName
import io.github.vigoo.zioaws.dynamodb.{createTable, putItem, query, DynamoDb}
import zio.console._
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, suite, _}
import zio.{Chunk, ZIO}

object StreamingQuerySpec extends DefaultRunnableSpec {

  val program: ZIO[DynamoDb, AwsError, Chunk[Map[AttributeName, AttributeValue.ReadOnly]]] = for {
    _ <- createTable(createTableRequest)
    _ <- ZIO.foreach_(1 to 5) { i =>
      putItem(putItemRequest(i))
    }
    response <- query(findAllByIdInTheLastYear(id = "id")).runCollect

  } yield response

  override def spec =
    suite(label = "StreamingQuerySpec")(
      testM("should return the results of a streamed query") {
        val value = (for {
          _ <- ZIO.service[DynamoDBProxyServer]
          items <- program
          rowCount = items.size
          _ <- putStrLn(s"total rows found=${items.size}")
          _ <- putStrLn(s"${items.toList}")
        } yield assert(rowCount)(equalTo(5)))
        value
      }.provideCustomLayerShared((ddbLayer ++ LocalDdbServer.inMemoryLayer).orDie)
    )
}
