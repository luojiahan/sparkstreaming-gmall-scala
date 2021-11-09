package com.cuiyf41.ods


import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.cuiyf41.util.{MyKafkaUtil, OffsetManagerUtil}
import com.cuiyf41.util.MyKafkaSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从kafka中读取数据，根据表名进行分流处理
 */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "gmall_db_c"
    var groupId = "base_db_canal_group"

    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap != null && offsetMap.size > 0) {
      //从指定的偏移量维值开始消费
      recordDStream = MyKafkaUtil.getKafkaDStream(ssc, topic, groupId, offsetMap)
    } else {
      //从最新的位置开始消费
      recordDStream = MyKafkaUtil.getKafkaDStream(ssc, topic, groupId)
    }

    //获取当前批次读取的kafka主题中偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对接收的数据进行结构的转换，consumerRecord[String, String(jsonStr)] => jsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取json格式的字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }

    //分流：根据不同的表名，将数据发送到不同的kafka主题中去
    jsonObjDStream.foreachRDD{
      rdd => {
        rdd.foreach{
          jsonObj => {
            //获取表名、操作类型、操作数据
            val opType: String = jsonObj.getString("type")
            if ("INSERT".equals(opType)) {
              val tableName: String = jsonObj.getString("table")
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              //拼接目标topic名称
              var sendTopic = "ods_" + tableName

              //对data数组进行遍历
              import scala.collection.JavaConverters._

              for (dataJson <- dataArr.asScala) {
                //根据表名将数据发送到不同的主题中
                MyKafkaSink.send(sendTopic, dataJson.toString)
              }
            }
          }
        }
        //提交偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}