package dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import zio.{Has, Task, UIO, ZIO, ZLayer}

object DynamoDBLocal {
  type DBServer = Has[Service]

  trait Service {
    def start: Task[Unit]
  }

  val inMemoryServer: ZLayer[Any, Throwable, Has[DynamoDBProxyServer]] =
    ZLayer.fromAcquireRelease[Any, Throwable, DynamoDBProxyServer] {
      UIO(println("Creating")) *> Task(createServer)
    } { server =>
      UIO(println("Stopping")) *> UIO(server.stop())
    }

  val live: ZLayer[Has[DynamoDBProxyServer], Nothing, Has[Service]] =
    ZLayer.fromService[DynamoDBProxyServer, DynamoDBLocal.Service] { server =>
      new DynamoDBLocal.Service {
        override def start: Task[Unit] = Task(server.start())
      }
    }

  def startDynamoDb: ZIO[DBServer, Throwable, Unit] = ZIO.accessM[DBServer](_.get.start)

  // TODO make private
  def createServer: DynamoDBProxyServer = {
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
