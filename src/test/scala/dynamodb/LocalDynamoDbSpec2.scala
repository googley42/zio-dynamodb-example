package dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import zio._

class LocalDynamoDbSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  private lazy val server: DynamoDBProxyServer = DBServer.createServer

  override def beforeAll(): Unit =
    server.start()

  override def afterAll(): Unit = server.stop()

  "zio" should {

    "stream dynamoDb table" in {
      Runtime.default.unsafeRun(Foo.createTableProgram)
    }
  }

}
