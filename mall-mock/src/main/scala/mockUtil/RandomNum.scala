package mockUtil

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * @author zxfcode
  * @create 2018-12-07 15:49
  */
object RandomNum {
  def apply(fromNum:Int,toNum:Int): Int =  {
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复
    if(canRepeat){
      val listValues: ListBuffer[Int] = ListBuffer[Int]()
      for(i <- 0 to amount){
        listValues+=apply(fromNum,toNum)
      }
      listValues.mkString(delimiter)
    }else{
      val setValues: mutable.HashSet[Int] = mutable.HashSet[Int]()
      while(setValues.size < amount){
        setValues += apply(fromNum,toNum)
      }
      setValues.mkString(delimiter)
    }

  }

//  def main(args: Array[String]): Unit = {
//    println(multi(1,5,5,",",true))
//  }
}
