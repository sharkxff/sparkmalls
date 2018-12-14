package realtime

import bean.CityUserClick
import commUtils.MallRedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis
/**
  * @author zxfcode
  * @create 2018-12-13 15:49
  */
object LastHourClickApp {
  //CityUserClick(date, area, city, userId, adsId)==>(key,adsid,json)
def getLastHourAdsCount(cityUserClickInfos: DStream[CityUserClick]): Unit ={
  //最近一小时广告点击量实时统计
  //需要用到spark的窗口函数，注意window大小和滑动步长
  val winUserClickInfos: DStream[CityUserClick] = cityUserClickInfos.window(Minutes(5),Seconds(10))
  //转换为(date_ads,count)=>(ads,(hour_min,count))=>(ads,iterable(hour_min,count))=>(ads,json)
  val hourMinCount: DStream[(String, Long)] = winUserClickInfos.map { adsClick =>
    val adsid: String = adsClick.asdid
    val hourMin: String = adsClick.getHourMinString()
    val hKey: String = adsid + "_" + hourMin
    (hKey, 1L)
  }.reduceByKey(_ + _)
  //(ads,iterable(hour_min,count))
  val groupBYAdsidCount: DStream[(String, Iterable[(String, Long)])] = hourMinCount.map { case (key, count) =>
    val keys: Array[String] = key.split("_")
    val adsid: String = keys(0)
    val hourMin: String = keys(1)
    (adsid, (hourMin, count))
  }.groupByKey()
  //通过json4s转换为json格式的数据
  //(adsis,hourminCountJson)
  val lastHourClickJson: DStream[(String, String)] = groupBYAdsidCount.map { case (adsid, itera) =>
    (adsid, compact(render(itera)))
  }
  //存入redis
  //(last_hour_ads_click,map(adsid,map(hour_min,count)))
  val jedis: Jedis = MallRedisUtil.getJedisClient
  lastHourClickJson.foreachRDD{rdd=>
    //需要收集rdd再存入redis
    val hour_click: Array[(String, String)] = rdd.collect()
    //需要把scala的Map转换为java的Map
    import collection.JavaConversions._
    jedis.hmset("last_hour_ads_click",hour_click.toMap)
  }

}
}
