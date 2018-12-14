package realtime

import java.util.Date

import bean.CityUserClick
import commUtils.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * @author zxfcode
  * @create 2018-12-13 8:55
  */
object RealTimeApp {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("blackuser")
    val sc = new SparkContext(sparkconf)
    val ssc = new StreamingContext(sc,Seconds(5))
    //获得生产者对应topic生产的数据
    val userClickInfo: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream("ads_log",ssc)
    val userInfoValue: DStream[String] = userClickInfo.map(input=>input.value())
    //将得到的数据存在样例类中
    val cityUserClickInfos: DStream[CityUserClick] = userInfoValue.map { rdd =>
      val userInfos: Array[String] = rdd.split(" ")
      //生产者生产的数据：array += timestamp + " " + area + " " + city + " " + userid + " " + adid
      val adsDate: String = userInfos(0)
      val area: String = userInfos(1)
      val city: String = userInfos(2)
      val userId: String = userInfos(3)
      val adsId: String = userInfos(4)
      val date = new Date(adsDate.toLong)
      CityUserClick(date, area, city, userId, adsId)
    }
    //黑名单人员再次点击广告时，广告点击数不再增加（需求七）
    //BlackUserApp.saveBlackUser(sc,ssc,cityUserClickInfos)
    //把汇总数据写入,写入的时流里的rdd(按周期写入)（需求八）
//    val dayClickAdsCount: DStream[(String, Long)] = RealtimeAdsClickColApp.getAdsClickTop(sc,ssc,cityUserClickInfos)
//    dayClickAdsCount.cache()
    //每天各地区 top3 热门广告（需求九）
    //以需求八的结果作为基础来做这个需求
//    HotAdsClickApp.getDaysTop3Ads(dayClickAdsCount)
//    println("需求九完成")

    LastHourClickApp.getLastHourAdsCount(cityUserClickInfos)
    println("需求十完成")
    ssc.start()
    ssc.awaitTermination()
  }
}
