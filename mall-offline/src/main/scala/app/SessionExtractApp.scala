package app

import java.text.SimpleDateFormat

import app.bean.UserSessionInfo
import commUtils.UserConfigUtil
import model.UserVisitAction
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * @author zxfcode
  * @create 2018-12-08 23:10
  */
object SessionExtractApp {
  val extractNum = 1000

  def sessionExtract(sparkSession: SparkSession,sessionCount: Long, taskId: String, userRdd: RDD[(String, Iterable[UserVisitAction])]) {
    //    1   所有session集合，以sessionId为单位
    //    2    求出抽取的个数
    //      某个小时要抽取得session个数=某个小时的session个数 /总session数量 *1000
    //    3 按个数去抽取
    //      RDD[sessionId,Iterable[UserAction]]
    //    按天+小时进行聚合 ，求出每个【天+小时】的session个数

    //    1  RDD[sessionId,Iterable[UserAction]]
    //    =>map=>
    val sessionInfoRdd: RDD[UserSessionInfo]  =  userRdd.map { case (sessionid, iterable) =>
      var maxTimeLength = -1L
      var minTimeLength = Long.MaxValue
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //把session里的操作放入集合里
      val keyWordList = ListBuffer[String]()
      val clickList = ListBuffer[String]()
      val orderList = ListBuffer[String]()
      val payList = ListBuffer[String]()
      //遍历iterable，把操作装进集合
      for (ite <- iterable) {
        val sessionTime: Long = format.parse(ite.action_time).getTime()
        maxTimeLength = Math.max(maxTimeLength, sessionTime)
        minTimeLength = Math.min(minTimeLength, sessionTime)
        if (ite.search_keyword != null) {
          keyWordList += ite.search_keyword
        }else if(ite.click_product_id != -1L){
          clickList += ite.click_product_id.toString
        }else if(ite.order_product_ids != null){
          orderList += ite.order_product_ids
        }else{
          payList += ite.pay_product_ids
        }
      }
        val sessionTimeLength = maxTimeLength - minTimeLength
        val stepLength: Int = iterable.size
        val startDate: String = format.format(minTimeLength)
        UserSessionInfo(taskId,sessionid,startDate,stepLength,sessionCount,keyWordList.mkString(","),
          clickList.mkString(","),orderList.mkString(","),payList.mkString(","))
    }
    //      2 RDD[ UserSessionInfo]
    val dayHourSession: RDD[(String, UserSessionInfo)] = sessionInfoRdd.map { session =>
      val dayHour: String = session.startTime.split(":")(0)
      (dayHour, session)
    }
    //      3 RDD[ day_hour,UserSessionInfo]
    //    =>groupbykey
    val dayHourGroup: RDD[(String, Iterable[UserSessionInfo])] = dayHourSession.groupByKey()
    //    4 RDD[day_hour, Iterable[UserSessionInfo]]  多个

    val sessionResult: RDD[UserSessionInfo] = dayHourGroup.flatMap { case (dayhour, iterables) =>
      //1确定抽取的个数  公式：    当前小时的session数 / 总session数  * 一共要抽取的数
      val hourRate: Int = Math.round(extractNum * iterables.size / sessionCount)
      //2 按照要求的个数进行抽取
      val infoes: mutable.HashSet[UserSessionInfo] = randomExtract(iterables.toArray, hourRate)
      infoes
    }
    //      =》抽取
    //    RDD[day_hour, Iterable[UserSessionInfo]]
    import sparkSession.implicits._
    val session2con: FileBasedConfiguration = UserConfigUtil("config.properties").config
    sessionResult.toDF.write.format("jdbc").option("url",session2con.getString("jdbc.url"))
          .option("user",session2con.getString("jdbc.user"))
          .option("password",session2con.getString("jdbc.password"))
            .option("dbtable","random_session_info")
          .mode(SaveMode.Append).save()
  }
  //抽取一个数
  def randomExtract[T](arr:Array[T],num:Int) ={
    //不可重复集合用来装抽取到的数据
    val sessionSet: mutable.HashSet[T] = mutable.HashSet[T]()
    while(sessionSet.size<num){
      val index: Int = new Random().nextInt(arr.length)
      sessionSet+=arr(index)
    }
    sessionSet
  }

}
