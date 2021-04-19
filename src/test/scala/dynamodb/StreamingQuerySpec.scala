package dynamodb

import dynamodb.AwsLayers.ddbLayer
import dynamodb.Requests._
import io.github.vigoo.zioaws.dynamodb.{createTable, putItem, query}
import zio.ZIO
import zio.console._
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, suite, _}

object StreamingQuerySpec extends DefaultRunnableSpec {

  val program = for {
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
          svr <- ZIO.service[LocalDdbServer.Service]
          _ <- svr.start
          items <- program
          rowCount = items.size
          _ <- putStrLn(s"total rows found=${items.size}")
          _ <- putStrLn(s"${items.toList}")
        } yield assert(rowCount)(equalTo(5)))
          .provideCustomLayer(ddbLayer ++ LocalDdbServer.live)
        value
      }
    )
}
