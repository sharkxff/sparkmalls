package realtime

import java.util.Date

import bean.CityUserClick
import commUtils.{MallRedisUtil, MyKafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * @author zxfcode
  * @create 2018-12-12 19:19
  */
object RealtimeAdsClickColApp {
  //得到每天点击量
  def getAdsClickTop(sc:SparkContext,ssc:StreamingContext,cityUserClick: DStream[CityUserClick])={
    val jedis: Jedis = MallRedisUtil.getJedisClient
    val cityClickCount: DStream[(String, Long)] = cityUserClick.map { cityClick =>
      (cityClick.getDateDayString() + ":" + cityClick.area + ":" + cityClick.city + ":" + cityClick.asdid, 1L)
    }.reduceByKey(_ + _)
    sc.setCheckpointDir("./checkpoint")
    // =>利用updateStatebykey 把历史数据进行汇总，得到最新的汇总数据，然后汇总数据保存到redis
    val seqCount: DStream[(String, Long)] = cityClickCount.updateStateByKey { (adsCountSeq: Seq[Long], totalCount: Option[Long]) =>
      val clickCount: Long = adsCountSeq.sum
      val daycount: Long = totalCount.getOrElse(0L) + clickCount
      Some(daycount)
    }
    writeIntoRedis(jedis,seqCount)
    seqCount
  }
  def writeIntoRedis(jedis: Jedis,seqCount: DStream[(String, Long)]) ={
    seqCount.foreachRDD{rdd=>
      val clickCount: Array[(String,Long)] = rdd.collect()
      for((key,count)<-clickCount){
        jedis.hincrBy("day_area_city_count",key,count)
      }
    }
  }
}
