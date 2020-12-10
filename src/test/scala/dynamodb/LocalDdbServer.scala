package dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import zio.{Has, UIO, ZIO, ZLayer}

object LocalDdbServer {
  type LocalDdbServer = Has[Service]

  trait Service {
    def start: UIO[Unit]
  }

  private val inMemoryServer: ZLayer[Any, Nothing, Has[DynamoDBProxyServer]] =
    ZLayer.fromAcquireRelease[Any, Nothing, DynamoDBProxyServer] {
      UIO(println("Creating local DynamoDb server")) *> UIO(createServer)
    } { server =>
      UIO(println("Stopping local DynamoDb server")) *> UIO(server.stop())
    }

  val live: ZLayer[Any, Nothing, Has[Service]] = inMemoryServer >>> ZLayer
    .fromService[DynamoDBProxyServer, LocalDdbServer.Service] { server =>
      new LocalDdbServer.Service {
        override def start: UIO[Unit] = UIO(server.start())
      }
    }

  def start: ZIO[LocalDdbServer, Nothing, Unit] = ZIO.accessM[LocalDdbServer](_.get.start)

  private def createServer: DynamoDBProxyServer = {
    System.setProperty("sqlite4java.library.path", "native-libs")
    System.setProperty("aws.accessKeyId", "dummy-key")
    System.setProperty("aws.secretKey", "dummy-key") // This is not used
    System.setProperty("aws.secretAccessKey", "dummy-key")

    import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
    import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
    // Create an in-memory and in-process instance of DynamoDB Local that runs over HTTP

    val localArgs = Array("-inMemory")
    val server: DynamoDBProxyServer = ServerRunner.createServerFromCommandLineArgs(localArgs)
    server
  }
}
