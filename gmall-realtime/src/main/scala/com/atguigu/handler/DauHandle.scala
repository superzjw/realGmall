package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandle {

  /**
    * 批次间去重
    * @param startUpLogDStream
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.filter(log=>{
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102",6379)
      //
      val redisKey: String = "Dau"+log.logDate
      //用sismember进行去重
      val boolean: Boolean = jedis.sismember(redisKey,log.mid)

      //关闭连接
      jedis.close()
      !boolean
    })

  }

  /**
    *将最终去重后的数据存入redis
    *
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd=>{
      //foreachRDD运行在driver端
      rdd.foreachPartition(partition=>{
        val jedis: Jedis = new Jedis("hadoop102",6379)
      //foreachPartition运行在Executor端
      partition.foreach(log=>{
        val redisKey: String = "Dau"+log.logDate
        jedis.sadd(redisKey,log.mid)

       })
        //关闭连接
        jedis.close()
      })
    })

  }


}
