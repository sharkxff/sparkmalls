package realtime

import java.util
import java.util.Date

import bean.CityUserClick
import commUtils.{MallRedisUtil, MyKafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @author zxfcode
  * @create 2018-12-12 15:09
  */
object BlackUserApp {
  def saveBlackUser(sc:SparkContext,ssc:StreamingContext,cityUserClickInfos: DStream[CityUserClick]) ={
    //黑名单人员再次点击广告时，广告点击数不再增加
    //思路：获取到数据后，先做黑名单
    //步骤：把数据做成(key,1L)的形式，聚合后是用户点击某条广告的次数，判断是否大于阈值，大于的加入黑名单
    //建立redis
    val jedis: Jedis = MallRedisUtil.getJedisClient
    //步骤：给用户做点击统计时，判断userid是否在黑名单里，在的就过滤掉不再统计他的点击次数
    //此处用transform比用filter好
    val userIsNotInBlack: DStream[CityUserClick] = cityUserClickInfos.transform { map =>
      //得到黑名单里的用户id
      val useridSet: util.Set[String] = jedis.smembers("ads_user_black")
      //用广播变量来传大数据集
      val userSetBroad: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(useridSet)
      val userInBlack: RDD[CityUserClick] = map.filter { cityuserClick =>
        //要用广播变量的值，需要用到value
        !userSetBroad.value.contains(cityuserClick.userid)
      }
      userInBlack
    }

    val userClickAdsCounts: DStream[(String, Long)] = userIsNotInBlack.map { userClick =>
      ( userClick.userid + ":" + userClick.asdid+":"+userClick.getDateString() , 1L)
    }.reduceByKey(_ + _)
    //聚合后是用户点击某条广告的次数，存入redis，判断是否大于阈值，大于的加入黑名单
    writeUserClickIntoRedis(jedis,userClickAdsCounts)
  }

  def writeUserClickIntoRedis(jedis: Jedis,userClickAdsCounts: DStream[(String, Long)])={
    userClickAdsCounts.foreachRDD{rdd=>
      val dateClick: Array[(String,Long)] = rdd.collect()

      for ((key,count) <- dateClick) {
        val adsClickCount: String = jedis.hget("city_user_click_count",key)
        if(adsClickCount != null && adsClickCount.toLong>30){
          val adsClickInfos: Array[String] = key.split(":")

          //println(adsClickInfos(0))
          jedis.sadd("ads_user_black",adsClickInfos(0))
        }
        jedis.hincrBy("city_user_click_count",key,count)
      }
    }
  }
}
