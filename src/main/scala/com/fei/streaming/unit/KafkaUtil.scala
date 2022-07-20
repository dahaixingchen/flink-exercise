package com.fei.streaming.unit

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

;

/**
  * @Description:
  * @ClassName: KafkaUtil
  * @Author chengfei
  * @DateTime 2022/1/25 14:47
  **/
object KafkaUtil {

  //  private val logger: Logger = LoggerFactory.getLogger(getClass)

  object AutoOffSetReset extends Enumeration {
    val earliest, latest = Value

    def withId(id: Int): Value = {
      values.find(_.id == id).get
    }
  }

  val consumerProps = new Properties()
  consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.191.80.157:9092")
  consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-exercise")
  consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffSetReset.latest.toString)
  consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

  val producerProps = new Properties()
  producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.191.80.157:9092")
  //EXACTLY_ONCE 的情况下，sink的超时时间要和broker中的一致，15min
  producerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000")
  producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5")
  producerProps.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Integer.MAX_VALUE.toString)
  producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd")
  producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all")

  def main(args: Array[String]): Unit = {
    //    logger.info(SystemProperties.flinkName)
  }
}
