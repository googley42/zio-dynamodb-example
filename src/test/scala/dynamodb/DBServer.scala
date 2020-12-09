package dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer

object DBServer {
  // TODO: make into a service and use ZLayer or ZManaged for resource safety

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
