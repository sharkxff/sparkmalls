package realtime

import commUtils.MallRedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis
/**
  * @author zxfcode
  * @create 2018-12-13 9:14
  */
object HotAdsClickApp {
def getDaysTop3Ads(dayClickAdsCount: DStream[(String, Long)])={
  //需求八结果dayClickAdsCount 2018-12-13:华北:北京:3 1612
  //每天各地区 top3 热门广告（需求九）
  //最终结果：key:top3_ads_per_day:2018-12-13 value:map(area,json)
  //转换成(date:area:ads,count)
  val dayAreaClickCount: DStream[(String, Long)] = dayClickAdsCount.map { case (day_area_city_ads, count) =>
    val daysAreaClickKey: Array[String] = day_area_city_ads.split(":")
    val date: String = daysAreaClickKey(0)
    val area: String = daysAreaClickKey(1)
    val ads: String = daysAreaClickKey(3)
    (date + ":" + area + ":" + ads, 1L)
  }.reduceByKey(_ + _)
  //转换成：(day,iterable(area,(ads,count)))
  val daysKeyAdsCount: DStream[(String, Iterable[(String, (String, Long))])] = dayAreaClickCount.map { case (dayAreaKey, count) =>
    val keysArr: Array[String] = dayAreaKey.split(":")
    val date: String = keysArr(0)
    val area: String = keysArr(1)
    val ads: String = keysArr(2)
    (date, (area, (ads, count)))
  }.groupByKey()
  //转换成(day,map(area,map(ads,count)))
  val hotAdsClickMap: DStream[(String, Map[String, String])] = daysKeyAdsCount.map { case (day, iterable) =>
    //把ads,count聚合成map，方便转换为json
    val areaAdsCountMap: Map[String, Iterable[(String, (String, Long))]] = iterable.groupBy { case (area, adsCount) => area }
    val areaAdsClickTop3: Map[String, String] = areaAdsCountMap.map { case (areas, iterable) =>
      //把iterable转换成（ads,count）
      val adsCount: Iterable[(String, Long)] = iterable.map { case (area, (adsis, count)) => (adsis, count) }
      //排序，取前两个
      val adsCountTop3: List[(String, Long)] = adsCount.toList.sortWith { (ads1, ads2) => ads1._2 > ads2._2 }.take(3)

      //用json4s将map转换成json
      val adsClickJsons: String = compact(render(adsCountTop3))
      (areas, adsClickJsons)
    }
    (day, areaAdsClickTop3)
  }
  hotAdsClickMap.foreachRDD{rdd=>
    rdd.foreachPartition{areaPartition=>
      val jedis: Jedis = MallRedisUtil.getJedisClient
      for ((daykey,mapvalue) <- areaPartition) {
        //jedis用的是java的map，这里返回的是scala的map，需要转换一下
        import collection.JavaConversions._
        jedis.hmset("day_area_ads_click_top3:"+daykey,mapvalue)
      }
      //每五秒就要用一个jedis，需要关闭，避免jedis不够用
      jedis.close()
    }
  }
}
}
