package com.yi.dw.common

import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object DateUtils {
  /**
    * 将字符串格式化成"yyyy-MM-dd HH:mm:ss"格式
    * @param stringDate
    * @return
    */
  def formateDate(stringDate:String): String ={
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var formateDate=""
    try {
      formateDate = sdf.format(sdf.parse(stringDate))
    } catch {
      case e:Exception =>{
        try {
          val bigDecimal = new BigDecimal(stringDate)
          val date = new Date(bigDecimal.longValue())
          formateDate = sdf.format(date)
        } catch {
          case e:Exception =>{
            formateDate
          }

        }

      }
    }

    formateDate
  }




  /**
    * 获取输入日期的前几天的日期
    * @param currentDate yyyyMMdd
    * @param i 获取前几天的日期
    * @return yyyyMMdd
    */
  def getCurrentDatePreDate(currentDate:String,i:Int): String ={
    val simpleDateFormat=new SimpleDateFormat("yyyyMMdd")
    val date:Date=simpleDateFormat.parse(currentDate)
    val calendar=Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE,-i)
    val per7Day=calendar.getTime
    simpleDateFormat.format(per7Day)

  }

}
