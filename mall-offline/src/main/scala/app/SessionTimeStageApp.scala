package app

import java.text.SimpleDateFormat

import app.catutils.UserAccumulator
import model.UserVisitAction
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @author zxfcode
  * @create 2018-12-10 19:02
  */
object SessionTimeStageApp {
def getStage(taskId:String,conditionJsonString: String,sessionCount:Long,
             userRdd: RDD[(String, Iterable[UserVisitAction])],accumulator:UserAccumulator)={
  //    遍历一下全部session，对每个session的类型进行判断 来进行分类的累加 （累加器）
  //    4 分类:时长,把session里面的每个action进行遍历,取出最大时间和最小时间,求差得到时长,再判断时长是否大于10秒
  //    步长： 计算下session中有多少个action, 判断个数是否大于5
  userRdd.foreach { case (sessionId, actions) =>
    var maxActionTine = -1L
    var minActionTime = Long.MaxValue
    var stepCount = actions.size
    for (action <- actions) {
      val dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val sessionTime: Long = dateformat.parse(action.action_time).getTime()
      maxActionTine = Math.max(sessionTime, maxActionTine)
      minActionTime = Math.min(sessionTime, minActionTime)
    }
    var visitTime: Long = maxActionTine - minActionTime
    if (visitTime <= 10000) {
      accumulator.add("session_visitLength_le_10_count")
    } else {
      accumulator.add("session_visitLength_gt_10_count")
    }
    if (stepCount <= 5) {
      accumulator.add("session_stepLength_le_5_count")
    } else {
      accumulator.add("session_stepLength_gt_5_count")
    }
  }
  //5 提取累加器中的值
  val accCount: mutable.HashMap[String, Long] = accumulator.value
  //6 把累计值计算为比例
  var session_visitLength_le_10_radio = Math.round(1000.0 * accCount("session_visitLength_le_10_count") / sessionCount) / 10.0
  var session_visitLength_gt_10_radio = Math.round(1000.0 * accCount("session_visitLength_gt_10_count") / sessionCount) / 10.0
  var session_stepLength_le_5_radio = Math.round(1000.0 * accCount("session_stepLength_le_5_count") / sessionCount) / 10.0
  var session_stepLength_gt_5_radio = Math.round(1000.0 * accCount("session_stepLength_gt_5_count") / sessionCount) / 10.0
  var result = Array(taskId, conditionJsonString, sessionCount, session_visitLength_le_10_radio
    , session_visitLength_gt_10_radio, session_stepLength_le_5_radio, session_stepLength_gt_5_radio)
  result
}
}
