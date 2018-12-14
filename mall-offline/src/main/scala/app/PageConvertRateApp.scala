package app

import com.alibaba.druid.support.json.JSONUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import commUtils.UserJdbcUtil
import model.UserVisitAction
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author zxfcode
  */
object PageConvertRateApp {
def statPageConvertRate(taskId:String,sparkSession:SparkSession,conditions:String,
                        userVisitRDD:RDD[UserVisitAction])= {
  //
  //val uservistiBroad: Broadcast[RDD[UserVisitAction]] = sparkSession.sparkContext.broadcast(userVisitRDD)

  //获取页面跳转
  val objPageJson: JSONObject = JSON.parseObject(conditions)
  //获取到页面转换成数组
  val targetPageJsons: Array[String] = objPageJson.getString("targetPageFlow").split(",")
  //通过zip组合成想要的数据
  val pagesZip: Array[(String, String)] = targetPageJsons.slice(0, targetPageJsons.length - 1)
    .zip(targetPageJsons.slice(1, targetPageJsons.length))

  //转换成page1-page2的类型
  val pagesPair: Array[String] = pagesZip.map { case (page1, page2) => page1 + "-" + page2 }
  //后续需要用到这个array，用广播变量来传这个值比较好
   val pageBroad: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageJsons.slice(0, targetPageJsons.length - 1))

  //单页访问次数
//过滤出符合条件的页面
  val userPageid: RDD[UserVisitAction] = userVisitRDD.filter {
    useraction => pageBroad.value.contains(useraction.page_id.toString)
  }
  //统计次数
  val pageCount: collection.Map[String, Long] = userPageid.map {
    uservisit => (uservisit.page_id.toString, 1L)
  }.countByKey()

  //通过sessionid聚合所有操作
  val sessionByKey: RDD[(String, Iterable[UserVisitAction])] = userVisitRDD.map {
    userActions =>
      (userActions.session_id, userActions)
  }.groupByKey()

  val pageIdPairs: RDD[(String, Long)] = sessionByKey.flatMap { case (sessionid, actionItera) =>
    //做时间排序，便于取得pageid对
    val actionSorted: List[UserVisitAction] = actionItera.toList.sortWith {
      (ac1, ac2) => ac1.action_time < ac2.action_time
    }
    //得到所有pageid
    val pageidList: List[Long] = actionSorted.map { action =>
      action.page_id
    }
    //得到一个session的所有pageid对
    val pageIdZips: List[String] = pageidList.slice(0, pageidList.length - 1).
      zip(pageidList.slice(1, pageidList.length))
      .map { case (page1, page2) => page1 + "-" + page2 }

    //过滤得到满足要求的pageid对
    val pagepairFilter: List[String] = pageIdZips.filter( pageid => pagesPair.contains(pageid))
    val pagepairs: List[(String, Long)] = pagepairFilter.map(x => (x, 1L))
    //pagepairs.foreach(println)
    pagepairs
  }

  val pagepairCounts: collection.Map[String, Long] = pageIdPairs.countByKey()
  //pagepairCounts.foreach(println)
  //pageCount.foreach(println)
  val result: Iterable[Array[Any]] = pagepairCounts.map { case (pageid, count) =>
    val prefixPage: String = pageid.split("-")(0)
    val preCount: Long = pageCount.getOrElse(prefixPage, 0L)
    val ratio: Double = Math.round(count / preCount.toDouble * 1000.0) / 10.0
    Array(taskId, pageid, ratio)
  }
  UserJdbcUtil.executeBatchUpdate("insert into page_convert_rate values(?,?,?)",result)

}

}
