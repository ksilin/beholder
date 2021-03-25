// *****************************************************************************
// Projects
// *****************************************************************************

lazy val beholder =
  project
    .in(file("."))
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= Seq(
        library.clients,
        library.kafka,
        library.betterFiles,
        library.avro4s,
        library.avro4sKafka,
        library.kafkaAvroSerializer,
        library.kafkaJsonSchemaSerializer,
        library.circe,
        library.circeGeneric,
        library.circeGenericExtras,
        library.circeParser,
        library.sttp,
        library.sttpBackendOkHttp,
        library.sttpCirce,
        library.airframeLog,
        library.logback,
        library.ksqlDbTestUtil,
        library.cloudEventsApi,
        library.cloudEventsJackson,
        library.cloudEventsKafka,
        library.config,
        library.scalatest % Test
      ),
      libraryDependencies ~= {
        _.map(_.exclude("org.slf4j", "slf4j-log4j12"))
      }
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val kafka       = "2.7.0"
      val confluent   = "6.0.1"
      val scalatest   = "3.2.0"
      val betterFiles = "3.9.1"
      val sttp        = "3.1.9"
      val circe       = "0.13.0"
      val avro4s      = "3.1.1"
      val airframeLog = "20.12.1"
      val cloudEvents = "2.0.0"
      val config      = "1.4.1"
      val logback     = "1.2.3"
    }

    val clients             = "org.apache.kafka"      % "kafka-clients"         % Version.kafka
    val kafka               = "org.apache.kafka"     %% "kafka"                 % Version.kafka
    val betterFiles         = "com.github.pathikrit" %% "better-files"          % Version.betterFiles
    val circe               = "io.circe"             %% "circe-core"            % Version.circe
    val circeGeneric        = "io.circe"             %% "circe-generic"         % Version.circe
    val circeGenericExtras  = "io.circe"             %% "circe-generic-extras"  % Version.circe
    val circeParser         = "io.circe"             %% "circe-parser"          % Version.circe
    val avro4s              = "com.sksamuel.avro4s"  %% "avro4s-core"           % Version.avro4s
    val avro4sKafka         = "com.sksamuel.avro4s"  %% "avro4s-kafka"          % Version.avro4s
    val kafkaAvroSerializer = "io.confluent"          % "kafka-avro-serializer" % Version.confluent
    val ksqlDbTestUtil      = "io.confluent.ksql"     % "ksqldb-test-util"      % Version.confluent
    val kafkaJsonSchemaSerializer =
      "io.confluent" % "kafka-json-schema-serializer" % Version.confluent
    val cloudEventsKafka  = "io.cloudevents"                 % "cloudevents-kafka" % Version.cloudEvents
    val cloudEventsApi    = "io.cloudevents"                 % "cloudevents-api"   % Version.cloudEvents
    val cloudEventsJackson    = "io.cloudevents"                 % "cloudevents-json-jackson"   % Version.cloudEvents
    val sttp              = "com.softwaremill.sttp.client3" %% "core"              % Version.sttp
    val sttpBackendOkHttp = "com.softwaremill.sttp.client3" %% "okhttp-backend"    % Version.sttp
    val sttpCirce         = "com.softwaremill.sttp.client3" %% "circe"             % Version.sttp
    val airframeLog       = "org.wvlet.airframe"            %% "airframe-log"      % Version.airframeLog
    val logback           = "ch.qos.logback"                 % "logback-classic"   % Version.logback
    val config            = "com.typesafe"                   % "config"            % Version.config
    val scalatest         = "org.scalatest"                 %% "scalatest"         % Version.scalatest
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val commonSettings =
  Seq(
    scalaVersion := "2.13.4",
    organization := "default",
    organizationName := "ksilin",
    startYear := Some(2021),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-encoding",
      "UTF-8",
      "-Ywarn-unused:imports"
    ),
    resolvers ++= Seq(
      "confluent" at "https://packages.confluent.io/maven",
      "ksqlDb" at "https://ksqldb-maven.s3.amazonaws.com/maven",
      "confluentJenkins" at "https://jenkins-confluent-packages-beta-maven.s3.amazonaws.com/6.1.0-beta200715032424/1/maven/",
      "confluentJenkins2" at "https://jenkins-confluent-packages-beta-maven.s3.amazonaws.com/6.1.0-beta200916191548/1/maven/",
      Resolver.sonatypeRepo("releases"),
      Resolver.bintrayRepo("wolfendale", "maven"),
      Resolver.bintrayRepo("ovotech", "maven"),
      "mulesoft" at "https://repository.mulesoft.org/nexus/content/repositories/public/",
      Resolver.mavenLocal
    ),
    scalafmtOnCompile := true
  )
