import com.example.{AuditLogEntry, TopicUtil}
import io.circe
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import io.cloudevents.CloudEvent
import io.cloudevents.core.format.EventFormat
import io.cloudevents.core.message.MessageReader
import io.cloudevents.kafka.impl.KafkaHeaders
import io.cloudevents.kafka.{CloudEventDeserializer, KafkaMessageFactory}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.Serdes
import io.cloudevents.jackson.JsonFormat



import java.util.Properties
import scala.jdk.CollectionConverters._

class AuditLogsCloudEventsTest extends TestBase {

  val auditLogTopic = "confluent-audit-log-events"

  val consumerGroup = s"${this.suiteName}_group"

  val cloudEventsJsonContentType = "application/cloudevents+json"

  // deserializing to CloudEvent used to fail bc of
  // Caused by: io.cloudevents.core.format.EventDeserializationException: com.fasterxml.jackson.databind.exc.MismatchedInputException: Invalid extensions name: confluentRouting
  // now, confluentRouting has been removed:

  "reads audit logs as CloudEvents" in {

    val props = commonConsumerProps.clone().asInstanceOf[Properties]
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[CloudEventDeserializer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)

    val consumer = new KafkaConsumer[String, CloudEvent](props)
    consumer.subscribe(List(auditLogTopic).asJava)
    TopicUtil.fetchAndProcessRecords(consumer)
    consumer.close()
  }

  "try the inner impl" in {

    val convertAndPrint: ConsumerRecord[String, Array[Byte]] => Unit = { rec =>
      val value: Array[Byte] = rec.value()
      val headers                   = rec.headers()

      val contentType: String = KafkaHeaders.getParsedKafkaHeader(headers, KafkaHeaders.CONTENT_TYPE)

      // as can also contain the charset and other stuff
      contentType must startWith(cloudEventsJsonContentType)

      val format: EventFormat = io.cloudevents.core.provider.EventFormatProvider.getInstance().resolveFormat(contentType);


      if (format != null) {
        // return structuredMessageFactory.apply(format);
      }

      val reader: MessageReader = KafkaMessageFactory.createReader(headers, value);

      val event: CloudEvent = reader.toEvent();
      info(s"event: $event")
    }

    val props = commonConsumerProps.clone().asInstanceOf[Properties]
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.ByteArray().deserializer().getClass)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)

    val consumer = new KafkaConsumer[String, Array[Byte]](props)
    consumer.subscribe(List(auditLogTopic).asJava)
    TopicUtil.fetchAndProcessRecords(consumer, convertAndPrint)
    consumer.close()
  }



}
