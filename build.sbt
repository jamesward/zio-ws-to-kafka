enablePlugins(JavaAppPackaging, DockerPlugin)

scalaVersion := "2.12.10"

val ZioVersion = "1.0.0-RC15"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % ZioVersion,
  "dev.zio" %% "zio-streams" % ZioVersion,
  "dev.zio" %% "zio-kafka" % "0.3.1",
  "com.softwaremill.sttp.client" %% "core" % "2.0.0-M7",
  "com.softwaremill.sttp.client" %% "async-http-client-backend-zio" % "2.0.0-M7",
  "com.softwaremill.sttp.client" %% "async-http-client-backend-zio-streams" % "2.0.0-M7",
  "org.slf4j" % "slf4j-simple" % "1.7.26",
)

dockerPermissionStrategy := com.typesafe.sbt.packager.docker.DockerPermissionStrategy.Run

dockerRepository := sys.props.get("docker.repo")

dockerUsername := sys.props.get("docker.username")

packageName := sys.props.get("docker.packagename").getOrElse(name.value)
