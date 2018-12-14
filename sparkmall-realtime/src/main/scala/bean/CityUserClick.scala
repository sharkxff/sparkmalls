package bean

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @author zxfcode
  * @create 2018-12-12 16:39
  */
case class CityUserClick(date:Date,area:String,city:String,userid:String,asdid:String) {
def getDateString() ={
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
   format.format(date)
}
  def getDateDayString() ={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.format(date)
  }
  def getHourMinString() ={
    val format = new SimpleDateFormat("HH:mm")
    format.format(date)
  }
}
