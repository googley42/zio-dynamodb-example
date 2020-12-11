package dynamodb

import dynamodb.AwsLayers.{ddbLayer, ddbLayer2}
import dynamodb.DynamoDb2.{query2, DynamoDb2}
import dynamodb.Requests._
import io.github.vigoo.zioaws.core.AwsError
import io.github.vigoo.zioaws.dynamodb.model.primitives.AttributeName
import io.github.vigoo.zioaws.dynamodb.{createTable, model, putItem, DynamoDb}
import zio.{Chunk, ZIO}
import zio.console._
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, suite, _}

object StreamingQuerySpec extends DefaultRunnableSpec {

  val program: ZIO[DynamoDb2 with DynamoDb, AwsError, Chunk[Map[AttributeName, model.AttributeValue]]] = for {
    _ <- createTable(createTableRequest)
    _ <- ZIO.foreach_(1 to 5) { i =>
      putItem(putItemRequest(i))
    }
    response <- query2(findAllByIdInTheLastYear(id = "id"), limit = 2).runCollect

  } yield response

  override def spec =
    suite(label = "StreamingQuerySpec")(
      testM("should return the results of a streamed query") {
        val value = (for {
          svr <- ZIO.service[LocalDdbServer.Service]
          _ <- svr.start
          items <- program
          rowCount = items.size
          _ <- putStrLn(s"total rows found=${items.size}")
          _ <- putStrLn(s"${items.toList}")
        } yield assert(rowCount)(equalTo(5)))
          .provideCustomLayer(ddbLayer ++ ddbLayer2 ++ LocalDdbServer.live)
        value
      }
    )
}
