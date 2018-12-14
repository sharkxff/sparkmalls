package app.catutils

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @author zxfcode
  * @create 2018-12-08 15:08
  */
class CatagoryAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
  var catagotyCount = new mutable.HashMap[String,Long]()
  //判断是否是初始值
  override def isZero: Boolean = {
    catagotyCount.isEmpty
  }
  //复制累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CatagoryAccumulator()
    accumulator.catagotyCount ++= catagotyCount
    accumulator
  }
//重置？？
  override def reset(): Unit = {
    catagotyCount = new mutable.HashMap[String,Long]()
  }
//计数
  override def add(key: String): Unit = {
    catagotyCount(key) = catagotyCount.getOrElse(key,0L)+1L
  }
//合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    //当前的map和传进来的map进行比较做累加
    val otherMap: mutable.HashMap[String, Long] = other.value
    //foldleft函数的作用顺序：用新传进来的值和上一轮的结果做运算
    catagotyCount=catagotyCount.foldLeft(otherMap){
      case(otherMap,(key,count))=>
        otherMap(key)=otherMap.getOrElse(key,0L)+count
        otherMap

    }
  }

  //得到当前值
  override def value: mutable.HashMap[String, Long] = {
    catagotyCount
  }
}
