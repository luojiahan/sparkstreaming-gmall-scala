package com.cuiyf41.dim

import com.alibaba.fastjson.JSON
import com.cuiyf41.bean.UserInfo
import com.cuiyf41.util.{MyKafkaUtil, OffsetManagerUtil}
import com.ibm.icu.text.SimpleDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Date

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    var topic = "ods_user_info"
    var groupId = "user_info_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size >0){
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

    val userInfoDStream: DStream[UserInfo] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])

        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val date: Date = dateFormat.parse(userInfo.birthday)
        val curTs: Long = System.currentTimeMillis()
        val betweenTms = curTs - date.getTime
        val age = betweenTms / 1000L / 60L / 60L / 24L / 365L
        if (age < 20) {
          userInfo.age_group = "20岁以下"
        } else if (age > 30) {
          userInfo.age_group = "30岁以上"
        } else {
          userInfo.age_group = "21岁到30岁"
        }
        if (userInfo.gender == "M") {
          userInfo.gender_name = "男"
        } else {
          userInfo.gender_name = "女"
        }
        userInfo
      }
    }

    userInfoDStream.foreachRDD{
      rdd => {
        import org.apache.phoenix.spark._
        rdd.saveToPhoenix(
          "GMALL_USER_INFO",
          Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME"),
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
