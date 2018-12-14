package app

import app.bean.CategoryTopNList
import app.catutils.CatagoryAccumulator
import model.UserVisitAction
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author zxfcode
  * @create 2018-12-10 11:52
  */
object CategoryTopNApp {
def topNBycategory(taskid:String,cateAccu:CatagoryAccumulator, userJsonValue: RDD[UserVisitAction])={
  //遍历获取到的用户访问操作，将操作加到对应的累加器中
  userJsonValue.foreach{userAction=>
    if(userAction.click_category_id != -1L){
      cateAccu.add(userAction.click_category_id+"_click")
    }else if(userAction.order_category_ids != null && userAction.order_category_ids.size != 0){
      val orders: Array[String] = userAction.order_category_ids.split(",")
      for(orderId <- orders){
        cateAccu.add(orderId+"_order")
      }
    } else if(userAction.pay_category_ids!=null&&userAction.pay_category_ids.size!=0){
      val parArr: Array[String] = userAction.pay_category_ids.split(",")
      for (elem <- parArr) {
        cateAccu.add(elem+"_pay")
      }
    }

  }
  //9_click -> 2360   9,(9_click,2360)//将单独的category对应的action数量转换为一个map
  val categoryGroupCid: Map[String, mutable.HashMap[String, Long]] = cateAccu.value.groupBy {
    case (cateId, count) =>
      val cid: String = cateId.split("_")(0)
      cid
  }
  //得到(taskid,cid,clickcount,ordercount,paycount)
  val cateNList: List[CategoryTopNList] = categoryGroupCid.map {
    case (key, cateMap) =>
      CategoryTopNList(taskid, key, cateMap.getOrElse(key + "_click", 0L),
        cateMap.getOrElse(key + "_order", 0L), cateMap.getOrElse(key + "_pay", 0L))
  }.toList
  //排序获取topN
  val cateSortTopN: List[CategoryTopNList] = cateNList.sortWith { (ct1, ct2) =>
    if (ct1.click_count < ct2.click_count) {
      false
    } else if (ct1.click_count == ct2.click_count) {
      if (ct1.order_count < ct2.order_count) {
        false
      } else if (ct1.order_count == ct2.order_count) {
        if (ct1.pay_count < ct2.pay_count) {
          false
        } else {
          true
        }
      } else {
        true
      }
    } else {
      true
    }
  }.take(10)
  cateSortTopN

}

}
