package com.cuiyf41.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * 创建kafaka DStream工具类
 */
object MyKafkaUtil {

  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")

  // kafka消费者配置
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "test_hdp_group",
    //latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
   * 创建DStream，返回接收到的输入数据 使用默认的消费者组
   * @param ssc: StreamingContext
   * @param topic: String
   * @return DStream: InputDStream[ConsumerRecord[String, String]]
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String): InputDStream[ConsumerRecord[String, String]] ={
    val DStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    DStream
  }

  /**
   * 在对Kafka数据进行消费的时候，指定消费者组
   * @param ssc: StreamingContext
   * @param topic: String
   * @param groupId: String
   * @return DStream: InputDStream[ConsumerRecord[String, String]]
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] ={
    kafkaParam.put("group.id", groupId)
    val DStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    DStream
  }

  /**
   * 从指定的偏移量位置读取数据
   * @param ssc: StreamingContext
   * @param topic: String
   * @param groupId: String
   * @param offsets: Map[TopicPartition, Long]
   * @return DStream: InputDStream[ConsumerRecord[String, String]]
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] ={
    kafkaParam.put("group.id", groupId)
    val DStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets)
    )
    DStream
  }
}