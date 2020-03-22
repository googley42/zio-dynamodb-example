name := "dynamodb"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += "DynamoDB Local Release Repository" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"
resolvers += Resolver.sonatypeRepo("releases")

val ZioVersion = "1.0.0-RC18-2"

libraryDependencies += "software.amazon.awssdk" % "dynamodb" % "2.8.7"
libraryDependencies += "com.amazonaws" % "DynamoDBLocal" % "1.11.477" % Test

libraryDependencies += "dev.zio" %% "zio" % ZioVersion
libraryDependencies += "dev.zio" %% "zio-test" % ZioVersion % Test
libraryDependencies += "dev.zio" %% "zio-streams" % ZioVersion

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// see http://softwarebyjosh.com/2018/03/25/how-to-unit-test-your-dynamodb-queries.html
// native libs
libraryDependencies += "com.almworks.sqlite4java" % "libsqlite4java-linux-i386" % "latest.integration" % "test"
libraryDependencies += "com.almworks.sqlite4java" % "libsqlite4java-linux-amd64" % "latest.integration" % "test"

lazy val copyJars = taskKey[Unit]("copyJars")
copyJars := {
  import java.nio.file.Files
  import java.io.File
  // For Local Dynamo DB to work, we need to copy SQLLite native libs from
  // our test dependencies into a directory that Java can find ("lib" in this case)
  // Then in our Java/Scala program, we need to set System.setProperty("sqlite4java.library.path", "lib");
  // before attempting to instantiate a DynamoDBEmbedded instance
  val artifactTypes = Set("dylib", "so", "dll")
  val files = Classpaths.managedJars(Test, artifactTypes, update.value).files
  Files.createDirectories(new File(baseDirectory.value, "native-libs").toPath)
  files.foreach { f =>
    val fileToCopy = new File("native-libs", f.name)
    if (!fileToCopy.exists()) {
      Files.copy(f.toPath, fileToCopy.toPath)
    }
  }
}

(compile in Compile) := (compile in Compile).dependsOn(copyJars).value
