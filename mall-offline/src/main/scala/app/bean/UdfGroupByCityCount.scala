package app.bean

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

/**
  * @author zxfcode
  */
class UdfGroupByCityCount extends UserDefinedAggregateFunction{
  //输入类型  sql里函数统计的字段
  override def inputSchema: StructType = StructType(Array(StructField("city_name",StringType)))
//缓存数据容器 城市点击count  和区域总count
  override def bufferSchema: StructType = StructType(Array(StructField("countMap",MapType(StringType,LongType))
  ,StructField("click_count",LongType)))
//输出数据类型
  override def dataType: DataType = StringType
//输入输出数据类型是否一致
  override def deterministic: Boolean = true
//初始化容器
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //udaf里的map是不可变的
    buffer(0) = new HashMap[String,Long]()
    //区域总点击数初始化为0
    buffer(1) = 0L
  }
//更新数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //每进来一个数据，map里对应的数据加一，对应区域count加一
    val countMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val clickCount: Long = buffer.getLong(1)
    val cityName: String = input.getString(0)
    buffer(1) = clickCount + 1L
    val newCountMap: Map[String, Long] = countMap + (cityName->(countMap.getOrElse(cityName,0L)+1L))
    buffer(0) = newCountMap
  }
//合并数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val countMap1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
    val count1: Long = buffer1.getLong(1)
    val rowMap: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)
    val count2: Long = buffer2.getLong(1)
    buffer1(1) = count1 + count2
    val newCountMap: Map[String, Long] = countMap1.foldLeft(rowMap) { case (map, (key, count)) =>
      map+(key->(map.getOrElse(key, 0L) + count))
    }
    buffer1(0) = newCountMap
  }
//显示最终结果  （(北京，1000)(辽宁，20000)），100000
  override def evaluate(buffer: Row): Any = {
    //city_click,count
    val clickCount: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    //得到总点击数
    val totalCount: Long = buffer.getLong(1)
    val cityClickList = new ListBuffer[CityRate]()
    //将citymap加进样例类中
    clickCount.foreach { case (key, count) =>
      val ratio: Double = Math.round(count/totalCount.toDouble*1000)/10.0
      cityClickList.append(CityRate(key,ratio))
    }
    //提取出排名靠前的两个城市
    val citySorted: ListBuffer[CityRate] = cityClickList.sortWith {
      case (city1, city2) => city1.rate > city2.rate
    }
    val cityTake2: ListBuffer[CityRate] = citySorted.take(2)
    //给定点击率为100
    var countRate = 100.0
    //处理前两名之后的数据
    if(citySorted.size>2){
      for (crate <- cityTake2) {
        countRate -= crate.rate
      }
      //formatted("%.2f")
      cityTake2.append(CityRate("其他",Math.round(countRate*10)/10.0))
      cityTake2.mkString(",")
    }else{
      cityTake2.mkString(",")
    }


  }

  case class CityRate(city_name:String,rate:Double){
    override def toString: String = {
      city_name+":"+rate+"%"
    }
  }
}
