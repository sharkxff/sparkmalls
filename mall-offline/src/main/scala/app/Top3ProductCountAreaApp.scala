package app

import java.util.UUID

import app.bean.UdfGroupByCityCount
import commUtils.UserConfigUtil
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author zxfcode
  * @create 2018-12-11 18:56
  */
object Top3ProductCountAreaApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("top2")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val taskId: String = UUID.randomUUID().toString
    //注册udaf函数
    spark.udf.register("group_city_count",new UdfGroupByCityCount())
    spark.sql("use sparkmall01")
    //先用action表和city表join得到count，再和project关联，得到商品名称
    //1.先获得有地区的访问记录
    val areacountdf: DataFrame = spark.sql("select u.click_product_id,c.area,c.city_name from user_visit_action u " +
      "join city_info c on u.city_id = c.city_id")
    //创建一个临时视图
    areacountdf.createOrReplaceTempView("tmp_area_product_click")

    val clickCountdf: DataFrame = spark.sql("select area,click_product_id,count(*) clickCount," +
      "group_city_count(city_name) city_remark from tmp_area_product_click group by area,click_product_id")
    clickCountdf.createOrReplaceTempView("tmp_area_product_count")

    val rkDf: DataFrame = spark.sql("select t2.*,rank() over(partition by t2.area order by t2.clickCount desc) rk " +
      "from tmp_area_product_count t2")
    rkDf.createOrReplaceTempView("rk_click_count")

    spark.sqlContext.udf.register("taskid", (str:String) => taskId)
    val rk3Df: DataFrame = spark.sql("select taskid(\"avc\") as taskId,t3.area,p.product_name,t3.clickCount,t3.city_remark from rk_click_count t3" +
      " join product_info p on t3.click_product_id = p.product_id where t3.rk <= 4")
    //rk3Df.show(100,false)

    import spark.implicits._
    val session2con: FileBasedConfiguration = UserConfigUtil("config.properties").config
    rk3Df.write.format("jdbc").option("url",session2con.getString("jdbc.url"))
      .option("user",session2con.getString("jdbc.user"))
      .option("password",session2con.getString("jdbc.password"))
      .option("dbtable","city_click_count_3")
      .mode(SaveMode.Append).save()
  }
}
