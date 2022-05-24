import com.example.{AuditLogEntry, TopicUtil}
import io.circe
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.parser._
import org.apache.kafka.clients.admin.{AdminClient, Config, DescribeConfigsResult, DescribeTopicsResult, TopicDescription}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.header.Header

import java.util
import java.util.Properties
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class AuditLogsConsumerTest extends TestBase {

  val auditLogTopic              = "confluent-audit-log-events"
  val cloudEventsJsonContentType = "application/cloudevents+json"
  val consumerGroup = this.suiteName

  private val auditLogTopicList: util.List[String] = List(auditLogTopic).asJava

  "read audit topic settings" in {
    val admin = AdminClient.create(commonConsumerProps)
    val desc: DescribeTopicsResult = admin.describeTopics(auditLogTopicList)
    val res: mutable.Map[String, TopicDescription] = Await.result(desc.all().toScalaFuture, 10.seconds).asScala
    println("audit topic desc:")
    res foreach println

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, auditLogTopic)

    val getConfig: DescribeConfigsResult = admin.describeConfigs(List(configResource).asJava)
    val res2: mutable.Map[ConfigResource, Config] = Await.result(getConfig.all().toScalaFuture, 10.seconds).asScala
    println("audit topic config:")
    res2 foreach println
  }


  "reads audit logs as string" in {
    val consumer = new KafkaConsumer[String, String](commonConsumerProps)
    consumer.subscribe(auditLogTopicList)
    TopicUtil.fetchAndProcessRecords(consumer)
  }

  // currently fails on confluentRouting field
  "must read audit logs as string, parse to JSON and log" in {

    val consumer = new KafkaConsumer[String, String](commonConsumerProps)
    consumer.subscribe(auditLogTopicList)

    val convertAndPrint: ConsumerRecord[String, String] => Unit = { rec =>
      val contentTypeHeader: Header = rec.headers().lastHeader("content-type")
      new String(contentTypeHeader.value()) must startWith(cloudEventsJsonContentType)
      println(s"decoding: ${rec.value()}")
      val parsedEvent: Either[circe.Error, AuditLogEntry] = decode[AuditLogEntry](rec.value())
      parsedEvent.fold(auditLogEntryErrorLogger, auditLogEntryStructuredLogger)
    }
    TopicUtil.fetchAndProcessRecords(
      consumer,
      convertAndPrint,
      abortOnFirstRecord = false,
      maxAttempts = Int.MaxValue
    )
  }

  "gambiarra analytics" in {
    val props = commonConsumerProps.clone().asInstanceOf[Properties]
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(auditLogTopicList)

    val records: Iterable[ConsumerRecord[String, String]] = TopicUtil.fetchAndProcessRecords(
      consumer,
      _ => (),
      abortOnFirstRecord = true,
      maxAttempts = 99
    )

    val entries: List[AuditLogEntry] =
      records.map(r => {
        val str = r.value()
        println(s"raw string: ${str}")
        val decoded = decode[AuditLogEntry](str)
        println(s"decoded: ${decoded}")
        decoded.right.get
      }).toList

    val simpleEntries: List[(String, String, String, String)] = entries.map { e =>
      val apiKeyOrUser = e.data.authenticationInfo.metadata
        .map(_.identifier)
        .getOrElse(e.data.authenticationInfo.principal)
      val resultOrError = e.data.result.map(_.status).getOrElse("NO RESULT")

      (e.data.methodName, apiKeyOrUser, e.time, resultOrError)
    }
    val groupedByMethod: Map[String, List[(String, String, String, String)]] =
      simpleEntries.groupBy(_._1)

    println("---")
    println("count by method:")
    println("---")
    groupedByMethod.view.mapValues(_.size).toList.sortBy(_._2).reverse foreach println

    val authNByUser: Map[String, List[(String, String, String, String)]] =
      groupedByMethod("kafka.Authentication").groupBy(_._2)

    val authNCountByUser: List[(String, (Int, String, String))] =
      authNByUser.view.mapValues(v => (v.size, v.map(_._3).min, v.map(_._3).max)).toList

    println("---")
    println("count by api key / user:")
    println("---")
    authNCountByUser.sortBy(_._2._1).reverse foreach (e => println(e.asJson.spaces2))
  }

  "identify events working with API keys" in {
    val props = commonConsumerProps.clone().asInstanceOf[Properties]
    // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(auditLogTopicList)

    val records: Iterable[ConsumerRecord[String, String]] = TopicUtil.fetchAndProcessRecords(
      consumer,
      _ => (),
      abortOnFirstRecord = false,
      maxAttempts = 99
    )

    val entries: List[AuditLogEntry] =
      records.map(r => decode[AuditLogEntry](r.value()).right.get).toList
  }

  "reads audit logs as JSON using KafkaJsonDeserializer - breaks java.util.LinkedHashMap cannot be cast to class com.example.package$AuditLogEntry" in {

    val jsonConfig = commonConsumerProps.clone().asInstanceOf[Properties]

    jsonConfig.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "io.confluent.kafka.serializers.KafkaJsonDeserializer"
    )

    // class java.util.LinkedHashMap cannot be cast to class com.example.package$AuditLogEntry (java.util.LinkedHashMap is in module java.base of loader 'bootstrap'; com.example.package$AuditLogEntry is in unnamed module of loader 'app')

    val consumer = new KafkaConsumer[String, AuditLogEntry](jsonConfig)

    consumer.subscribe(auditLogTopicList)

    val printAuthInfo: ConsumerRecord[String, AuditLogEntry] => Unit = { r =>
      val logEvent = r.value()
      info(logEvent.time)
      info(logEvent.data.authenticationInfo.principal)
      info(logEvent.data.authenticationInfo.metadata.map(_.identifier).getOrElse("NONE"))
    }
    TopicUtil.fetchAndProcessRecords(consumer, printAuthInfo)
  }

  val auditLogEntryErrorPrinter: circe.Error => Unit = (e: circe.Error) =>
    println(s"parsing failed: ${e}")
  val auditLogEntryPrinter: AuditLogEntry => Unit = (logEvent: AuditLogEntry) =>
    println(
      s"action: ${logEvent.data.methodName}, serviceName: ${logEvent.data.serviceName}, subject: ${logEvent.subject}, principal: ${logEvent.data.authenticationInfo.principal}, id/key: ${logEvent.data.authenticationInfo.metadata
        .map(_.identifier)
        .getOrElse("NONE")} ts: ${logEvent.time}"
    )

  val auditLogEntryErrorLogger: circe.Error => Unit = (e: circe.Error) =>
    warn(s"parsing failed: ${e}")
  val auditLogEntryLogger: AuditLogEntry => Unit = (logEvent: AuditLogEntry) =>
    info(
      s"action: ${logEvent.data.methodName}, serviceName: ${logEvent.data.serviceName}, subject: ${logEvent.subject},  principal: ${logEvent.data.authenticationInfo.principal}, id/key: ${logEvent.data.authenticationInfo.metadata
        .map(_.identifier)
        .getOrElse("NONE")} ts: ${logEvent.time}"
    )

  val auditLogEntryStructuredLogger: AuditLogEntry => Unit = (logEvent: AuditLogEntry) =>
    info(logEvent.asJson.deepDropNullValues.spaces2)

}
