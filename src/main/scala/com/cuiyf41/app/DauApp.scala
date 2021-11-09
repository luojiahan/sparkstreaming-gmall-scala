package com.cuiyf41.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cuiyf41.bean.DauInfo
import com.cuiyf41.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "gmall_start"
    val groupId = "test-hdp-group"


    //从Redis中获取Kafka分区偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size >0){
      //如果Redis中存在当前消费者组对该主题的偏移量信息，那么从执行的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaDStream(ssc,topic,groupId,offsetMap)
    }else{
      //如果Redis中没有当前消费者组对该主题的偏移量信息，那么还是按照配置，从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaDStream(ssc,topic,groupId)
    }

    //获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        //因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
//    val kafkaDstream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
//    kafkaDstream.print()

    val jsonObjDStream = offsetDStream.map{
      record =>
        val jsonString = record.value()
        val jsonObject = JSON.parseObject(jsonString)
        val ts = jsonObject.getLong("ts")
        val dateStr = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr = dateStr.split(" ")
        val dt = dateStrArr(0)
        val hr = dateStrArr(1)
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
    }
    /*
    val filteredDStream = jsonObjDStream.filter{
      Obj =>
        val jedis: Jedis = CuiyfRedisUtil.getJedisClient()
        val dt = Obj.getString("dt")
        val mid = Obj.getJSONObject("common").getString("mid")
        val dauKey = "dau:" + dt
        if(jedis.ttl(dauKey) < 0){
          jedis.expire(dauKey, 3600*24)
        }
        val isFrist: lang.Long = jedis.sadd(dauKey, mid)
        jedis.close()
        if (isFrist == 1L){
          true
        }else{
          false
        }
    }
     */

    val filteredDStream = jsonObjDStream.mapPartitions{
      jsonObjItr =>
        val jedis = MyRedisUtil.getJedisClient()
        val filteredList = new ListBuffer[JSONObject]()
        for(jsonobj <- jsonObjItr){
          val dt = jsonobj.getString("dt")
          val mid = jsonobj.getJSONObject("common").getString("mid")
          val dauKey = "dau:" + dt
          if(jedis.ttl(dauKey) < 0){
            jedis.expire(dauKey, 3600*24)
          }
          val isFirst = jedis.sadd(dauKey, mid)

          if (isFirst==1) {
            filteredList.append(jsonobj)
          }
        }
        jedis.close()
        filteredList.toIterator
    }
//    jsonObjDStream.print()

    filteredDStream.foreachRDD{
      rdd=>{
        //以分区为单位对数据进行处理
        rdd.foreachPartition{
          jsonObjItr=>{
            val dauInfoList: List[(String, DauInfo)] = jsonObjItr.map {
              jsonObj => {
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00",
                  jsonObj.getLong("ts")
                )
                (dauInfo.mid,dauInfo)
//                dauInfo
              }
            }.toList

            //将数据批量的保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList,"gmall_dau_info_" + dt)
          }
        }
        //提交偏移量到Redis中
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
