package dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import zio.{test, UIO, ZIO, ZManaged}
import zio.console._
import zio.test.{DefaultRunnableSpec, suite, _}

object StreamingQuerySpec extends DefaultRunnableSpec {
  private lazy val server: DynamoDBProxyServer = DynamoDBLocal.createServer

  val ddbLayer = DynamoDBLocal.inMemoryServer >>> DynamoDBLocal.live

  override def spec =
    suite(label = "StreamingQuerySpec")(
      testM("should return the results of a raw query") {
        for {
//          result <- Foo.rawQueryProgram
          _ <- putStrLn(s"1 XXXXXXXXXXXXXXXX")
        } yield assertCompletes
      },
      testM("should return the results of a streamed query") {
        for {
//          result <- Foo.streamedQueryProgram
          _ <- putStrLn(s"2 XXXXXXXXXXXXXXXX")
        } yield assertCompletes
      }
    ) @@ TestAspect2.aroundAll(DynamoDBLocal.startDynamoDb.provideLayer(ddbLayer))(_ => ZIO.unit)
}
