package com.cuiyf41.dim

import com.alibaba.fastjson.JSON
import com.cuiyf41.bean.ProvinceInfo
import com.cuiyf41.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ProvinceInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_base_province"
    var groupId = "province_info_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0){
      recordDStream = MyKafkaUtil.getKafkaDStream(ssc, topic, groupId, offsetMap)
    } else {
      recordDStream = MyKafkaUtil.getKafkaDStream(ssc, topic, groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val provinceInfoDStream: DStream[ProvinceInfo] = offsetDStream.map(
      record => {
        val jsonStr: String = record.value()
        val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
        provinceInfo
      }
    )
    provinceInfoDStream.foreachRDD{
      rdd => {
        import org.apache.phoenix.spark._
        rdd.saveToPhoenix(
          "GMALL_PROVINCE_INFO",
          Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"),
          new Configuration,
          Some("hdp101,hdp102,hdp103:2181")
        )
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
