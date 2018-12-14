package app

import java.util.UUID

import app.bean.{CategoryClickTopN, CategoryTopNList}
import com.alibaba.fastjson.{JSON, JSONObject}
import model.UserVisitAction
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import catutils.{CatagoryAccumulator, UserAccumulator}
import commUtils.UserConfigUtil

/**
  * @author zxfcode
  * @create 2018-12-08 15:20
  */
object OfflineApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("offlineapp").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //注册session累加器
    val accumulator = new UserAccumulator()
    sparkSession.sparkContext.register(accumulator)
    val taskId: String = UUID.randomUUID().toString
    val conditionConfig: FileBasedConfiguration = UserConfigUtil("condition.properties").config
    //将过滤条件加载出来
    val conditionJsonString: String = conditionConfig.getString("condition.params.json")
    //    1 \ 筛选  要关联用户  sql   join user_info  where  contidition  =>DF=>RDD[UserVisitAction]
    val userJsonValue: RDD[UserVisitAction] = readUserJson(sparkSession, conditionJsonString)
    //      2  rdd=>  RDD[(sessionId,UserVisitAction)] => groupbykey => RDD[(sessionId,iterable[UserVisitAction])]
    val userRdd: RDD[(String, Iterable[UserVisitAction])] = userJsonValue.map(x => (x.session_id, x)).groupByKey()
    userRdd.cache()
    //      3 求 session总数量，
    val sessionCounT: Long = userRdd.count()
    //
     val result: Array[Any] = SessionTimeStageApp.getStage(taskId,conditionJsonString,
       sessionCounT,userRdd,accumulator)
    //7 保存到mysql中
    //UserJdbcUtil.executeUpdate("insert into session_stat_info values(?,?,?,?,?,?,?)", result)

    //第二个需求
    //SessionExtractApp.sessionExtract(sparkSession,sessionCounT,taskId,userRdd)

    //需求三 获取点击、下单和支付数量排名前 10 的品类
    //用前面缓存的值userRdd来做,(session,iterable)=>map(cateid,list(cate_click,cate_order,cate_pay))
    //注册  品类累加器
    val cateAccu = new CatagoryAccumulator()
    sparkSession.sparkContext.register(cateAccu)

    val cateTopN10: List[CategoryTopNList] = CategoryTopNApp.topNBycategory(taskId,cateAccu,userJsonValue)
    //cateTopN10.foreach(println)
    //将list内的数据转成array的，便于jdbcutil操作多条数组数据
//    val cateList = new ListBuffer[Array[Any]]()
//    for (elem <- cateTopN10) {
//      val param = Array(elem.task_id,elem.category_id,elem.click_count,elem.order_count,elem.pay_count)
//      cateList.append(param)
//    }
   //UserJdbcUtil.executeBatchUpdate("insert into cate_top_10 values(?,?,?,?,?)",cateList)
    //Top10 热门品类中 Top10 活跃 Session 统计
    val  config: FileBasedConfiguration = UserConfigUtil("config.properties").config
      //CateSessionTopNApp.getTopNSession(taskId,sparkSession,config,cateTopN10,userJsonValue)

    //需求五
    PageConvertRateApp.statPageConvertRate(taskId,sparkSession,conditionJsonString,userJsonValue)
  }

  def readUserJson(sparkSession: SparkSession, conditionJsonString: String) = {
    //指定要使用哪个数据库
    val baseConfig: FileBasedConfiguration = UserConfigUtil("config.properties").config
    val database: String = baseConfig.getString("hive.database")
    sparkSession.sql("use " + database)
    //根据查询条件写sql获取值
    val jsonObj: JSONObject = JSON.parseObject(conditionJsonString)
    //    json.getString("startDate")

    var sql = new StringBuffer("select v.* from user_visit_action v join user_info u on v.user_id=u.user_id where 1=1")
    if (jsonObj.getString("startDate") != null) {
      sql.append(" and v.date>='" + jsonObj.getString("startDate") + "'")
    }
    if (jsonObj.getString("endDate") != null) {
      sql.append(" and v.date<='" + jsonObj.getString("endDate") + "'")
    }
    if (jsonObj.getString("startAge") != null) {
      sql.append(" and u.age>='" + jsonObj.getString("startAge") + "'")
    }
    if (jsonObj.getString("endAge") != null) {
      sql.append(" and u.age<='" + jsonObj.getString("endAge") + "'")
    }
    if (!jsonObj.getString("professionals").isEmpty) {
      sql.append(" and u.professional in( " + jsonObj.getString("professionals") + ")")
    }
    //sql.append(" limit 5")
    import sparkSession.implicits._
    val userValues: RDD[UserVisitAction] = sparkSession.sql(sql.toString).as[UserVisitAction].rdd
    userValues
  }
}
