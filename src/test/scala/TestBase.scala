import com.example.FutureConverter
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import java.util.{Properties, UUID}

class TestBase     extends AnyFreeSpec
  with Matchers
  with BeforeAndAfterAll
  with FutureConverter {

  val conf: Config = ConfigFactory.load()
  val ctx = "cloud"
  private val bootstrapPrefix: String = conf.getString(s"${ctx}.bootstrapPrefix")
  private val apiKey: String = conf.getString(s"${ctx}.apiKey")
  private val secret: String = conf.getString(s"${ctx}.secret")

  val commonConsumerProps = new Properties()
  commonConsumerProps.put(
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
    s"${bootstrapPrefix}.confluent.cloud:9092"
  )
  commonConsumerProps.put(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  commonConsumerProps.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer"
  )

  // commonConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  commonConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  commonConsumerProps.put(
    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
    "SASL_SSL"
  )
  commonConsumerProps.put(
    ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG,
    "use_all_dns_ips"
  )
  commonConsumerProps.put(
    SaslConfigs.SASL_JAAS_CONFIG,
    s"org.apache.kafka.common.security.plain.PlainLoginModule   required username='${apiKey}'   password='${secret}';"
  )
  commonConsumerProps.put(
    SaslConfigs.SASL_MECHANISM,
    "PLAIN"
  )
  commonConsumerProps.put(
    ConsumerConfig.CLIENT_ID_CONFIG,
    "audit_logs_beholder"
  )
  commonConsumerProps.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    s"audit_logs_beholder_group_${UUID.randomUUID()}"
  )

}
