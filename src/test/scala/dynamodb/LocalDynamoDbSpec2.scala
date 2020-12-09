package dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import io.github.vigoo.zioaws.dynamodb.model.{AttributeValue, QueryResponse}
import io.github.vigoo.zioaws.dynamodb.model.primitives.AttributeName
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import zio._

import scala.collection.immutable

class LocalDynamoDbSpec2 extends WordSpec with Matchers with BeforeAndAfterAll {
  private lazy val server: DynamoDBProxyServer = DBServer.createServer

  override def beforeAll(): Unit = server.start()

  override def afterAll(): Unit = server.stop()

  "zio" should {
//zio.stream.ZStream[DynamoDb, AwsError, Map[primitives.AttributeName, AttributeValue.ReadOnly]]
    "stream dynamoDb table" in {
      val x: Chunk[Map[AttributeName, AttributeValue]] = Runtime.default.unsafeRun(Foo.streamedQueryProgram)
      println(x)
    }
  }

}
