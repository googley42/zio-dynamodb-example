package dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import zio.UIO
import zio.console._
import zio.test.TestAspect._
import zio.test.{DefaultRunnableSpec, suite, _}

object StreamingQuerySpec extends DefaultRunnableSpec {
  private lazy val server: DynamoDBProxyServer = DBServer.createServer

  override def spec =
    suite(label = "StreamingQuerySpec")(
      testM("should return the results of a raw query") {
        for {
          result <- Foo.rawQueryProgram
          _ <- putStrLn(s"XXXXXXXXXXXXXXXX $result")
        } yield assertCompletes
      },
      testM("should return the results of a streamed query") {
        for {
          result <- Foo.streamedQueryProgram
          _ <- putStrLn(s"XXXXXXXXXXXXXXXX $result")
        } yield assertCompletes
      } @@ ignore
    ) @@ before(UIO(server.start)) @@ after(UIO(server.stop))
}
