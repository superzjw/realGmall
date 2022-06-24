package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandle
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf配置文件
    val sparkconf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    //2.创建SparkStreaming
    val ssc: StreamingContext = new StreamingContext(sparkconf,Seconds(3))
    //3.通过kafka工具类消费启动日志数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)
    //4.将消费到的json字符串转为样例类,并补全lodate&logHour两个字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将json字符串转为样例类
        val log: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        //补全两个字段
        val times: String = sdf.format(new Date(log.ts))

        log.logDate = times.split(" ")(0)
        log.logHour = times.split(" ")(1)

        log
      })
    })
    startUpLogDStream

    //去重前数据
    startUpLogDStream.cache()

    startUpLogDStream.count().print()


    //5.对相同日期,相同mid进行批次间去重
    val filterDStream: DStream[StartUpLog] = DauHandle.filterByRedis(startUpLogDStream)

    filterDStream.cache()

    filterDStream.count().print()

    //6.再对进行批次间去重的数据进行批次内去重

    //7.将最终去重后的数据存入redis
    DauHandle.saveToRedis( startUpLogDStream)



    //6.启动并阻塞
    ssc.start()
    ssc.awaitTermination()

  }
}








































