package app

import app.bean.{CategoryClickTopN, CategoryTopNList}
import model.UserVisitAction
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @author zxfcode
  */
object CateSessionTopNApp {
def getTopNSession(taskid:String,sparkSession: SparkSession,config:FileBasedConfiguration,
                   cateTopN: List[CategoryTopNList],userJsonValue: RDD[UserVisitAction])={
  //传递的是数据集，用广播比较合适
  val cateBroad: Broadcast[List[CategoryTopNList]] = sparkSession.sparkContext.broadcast(cateTopN)
  //过滤出前有十品类的action==>RDD[UserVisitAction]
  val sessionFilter: RDD[UserVisitAction] = userJsonValue.filter { userSession =>
    var flag = false
    //注意字段的类型是否一致，不一致会把数据过滤光，造成没有数据的情况
    for (cate <- cateBroad.value) {
      if (userSession.click_category_id.toString == cate.category_id) flag = true
    }
    flag
  }
  //相同的cid+sessionId进行累加计数
  val cidCount: RDD[(String, Long)] = sessionFilter.map { session =>
    (session.click_category_id + "_" + session.session_id, 1L)
  }.reduceByKey(_ + _)
  //根据cid聚合  (cid,(session,click_count))
  val cidGroupByKey: RDD[(String, Iterable[(String, Long)])] = cidCount.map {
    case (key, count) =>
    val keys: Array[String] = key.split("_")
    val cid: String = keys(0)
    val sessionid: String = keys(1)
    (cid, (sessionid, count))
  }.groupByKey()
  //println(cidGroupByKey.collect().mkString("\n"))
  //(cid,((30,200),(20,1000),(3,100)))==>(cid,(20,1000),(30,200),(3,100))
  val cidSessionClickTopN: RDD[CategoryClickTopN] = cidGroupByKey.flatMap {
    case (key, iterable) =>
    //给iterable排序取出前十
    val clickTop10: List[(String, Long)] = iterable.toList.sortWith {
      (ite1, ite2) => ite1._2 > ite2._2
    }.take(10)
    //放进对象里，方便传入数据库,根据需要的字段
    val cidClickTopN: List[CategoryClickTopN] = clickTop10.map {
      case (sessionid, count) => CategoryClickTopN(taskid, key, sessionid, count)
    }
    cidClickTopN
  }
  //println(cidSessionClickTopN.collect().mkString("\n"))
  import sparkSession.implicits._
  cidSessionClickTopN.toDF.write.format("jdbc").option("url",config.getString("jdbc.url"))
    .option("user",config.getString("jdbc.user")).option("password",config.getString("jdbc.password"))
    .option("dbtable","cate_click_top").mode(SaveMode.Append).save()
}


}
