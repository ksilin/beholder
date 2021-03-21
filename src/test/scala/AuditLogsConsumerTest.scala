import com.example.{ AuditLogEntry, TopicUtil }
import io.circe
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.parser._
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord, KafkaConsumer }

import java.util.Properties
import scala.jdk.CollectionConverters._

class AuditLogsConsumerTest extends TestBase {

  val auditLogTopic = "confluent-audit-log-events"

  "reads audit logs as string" in {

    val consumer = new KafkaConsumer[String, String](commonConsumerProps)
    consumer.subscribe(List(auditLogTopic).asJava)
    TopicUtil.fetchAndProcessRecords(consumer)
  }

  "must read audit logs as string, parse to JSON and log" in {

    val consumer = new KafkaConsumer[String, String](commonConsumerProps)
    consumer.subscribe(List(auditLogTopic).asJava)

    val convertAndPrint: ConsumerRecord[String, String] => Unit = { rec =>
      val value                                      = rec.value()
      val parsed: Either[circe.Error, AuditLogEntry] = decode[AuditLogEntry](value)
      parsed.fold(auditLogEntryErrorLogger, auditLogEntryStructuredLogger)
    }
    TopicUtil.fetchAndProcessRecords(
      consumer,
      convertAndPrint,
      abortOnFirstRecord = false,
      maxAttempts = Int.MaxValue
    )
  }

  "reads audit logs as JSON using KafkaJsonDeserializer - breaks java.util.LinkedHashMap cannot be cast to class com.example.package$AuditLogEntry" in {

    val jsonConfig = commonConsumerProps.clone().asInstanceOf[Properties]

    jsonConfig.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "io.confluent.kafka.serializers.KafkaJsonDeserializer"
    )

    // class java.util.LinkedHashMap cannot be cast to class com.example.package$AuditLogEntry (java.util.LinkedHashMap is in module java.base of loader 'bootstrap'; com.example.package$AuditLogEntry is in unnamed module of loader 'app')

    val consumer = new KafkaConsumer[String, AuditLogEntry](jsonConfig)

    consumer.subscribe(List(auditLogTopic).asJava)

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
