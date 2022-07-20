package com.fei.streaming.cep

import com.fei.streaming.unit.{KafkaUtil, OrderInfo, OrderMessage}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.Map
;

/**
  * @Description:
  * @ClassName: SimpDemo
  * @Author chengfei
  * @DateTime 2022/7/20 11:28
  **/
object SimpDemo {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    //自定义端口
    conf.setInteger(RestOptions.PORT, 8050)
    val config = new ExecutionConfig
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    //创建一个侧输出流的Tag
    val tag = new OutputTag[OrderMessage]("pay_timeout")

    val stream: DataStream[OrderInfo] = env.addSource(new FlinkKafkaConsumer[OrderInfo](
      "order.log"
      , new KafkaDeserializationSchema[OrderInfo]() {
        override def isEndOfStream(nextElement: OrderInfo): Boolean = {
          return false;
        }

        override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): OrderInfo = {
          val arr: Array[String] = new String(record.value()).split(",")
          return OrderInfo(arr(0), arr(1), arr(2), arr(3).toLong, arr(4).toLong)
        }

        override def getProducedType: TypeInformation[OrderInfo] = {
          return TypeInformation.of(classOf[OrderInfo])
        }
      }
      , KafkaUtil.consumerProps
    )).assignAscendingTimestamps(_.endTime * 1000)

    val pattern: Pattern[OrderInfo, OrderInfo] = Pattern.begin[OrderInfo]("begin")
      .where(_.status.equals("create"))
      .followedBy("second")
      .where(_.status.equals("pay"))
      .within(Time.seconds(10))

    val pp: PatternStream[OrderInfo] = CEP.pattern(stream.keyBy(_.oid), pattern)

    val mainStream: DataStream[OrderMessage] = pp.select(tag)(
      (map: Map[String, Iterable[OrderInfo]], time: Long) => { //支付超时的数据处理
        val order: OrderInfo = map("begin").iterator.next()
        new OrderMessage(order.oid, "该订单在15分钟内没有支付，请尽快支付！", order.actionTime, 0)
      }
    )(
      (map: Map[String, Iterable[OrderInfo]]) => { //在15分钟内正常支付的订单数据处理
        val create: OrderInfo = map("begin").iterator.next()
        val pay: OrderInfo = map("second").iterator.next()
        new OrderMessage(create.oid, "订单正常支付，请尽快发货!", create.actionTime, pay.actionTime)
      }
    )

    mainStream.getSideOutput(tag).print("侧流")
    mainStream.print("主流")

    env.execute()
  }
}
