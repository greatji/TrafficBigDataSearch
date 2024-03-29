package org.dbgroup.trafficbigdata.avgspeed

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.joda.time._
import org.joda.time.format._

import scala.collection.JavaConverters._

/**
  * Created by sunji on 16/12/12.
  */
// 构造数据表schema
case class SPEED_BASE(LXBM: String, GDCSYZH: String, GDCSYBM: String, CSFX: String, CSYMC: String, JDZBS: String, LON: Double, LAT: Double)
case class SPEED_DATA(SITE_GUID: String, HPHM: String, WZSJMillis: Long, WZSJHourOfDay: Int, WZSJDayOfMonth: Int, WZSMonthOfYear: Int, WZSJYear: Int, CLSD: Int, ISOVERSPEED: Int)
case class FEE_DATA(EXSTATION: String, EXTIMEMillis: Long, ENSTATION: String, ENTIMEMillis: Long, EXVEHCLASS: String, ENVEHPLATE: String, EXVEHPLATE: String, EXTRUCKFLAG: String)

object AverageSpeed {
  def getAverageSpeed(sparkContext: SparkContext, sqlContext: SQLContext, data_path_base: String, lon_upper: Double, lon_lower: Double, lat_upper: Double, lat_lower: Double, date: String): java.util.List[String] = {

    // 数据预处理
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val end = DateTime.parse(date, fmt)
    var start = end.minusDays(30)
    val day = start.getDayOfMonth
    val month = start.getMonthOfYear
    val year = start.getYear
    //    println(prevTime.getMillis + ", " + prevMillis + ", " + tomorrowMillis)

    // today
    val thisMonthFilePath_today = (end.getYear * 100 + end.getMonthOfYear).toString
    val speed_base_path = data_path_base + "/speed_base.csv"
    val speed_data_path = data_path_base + "/" + thisMonthFilePath_today + "/" + thisMonthFilePath_today + "CSYDATA.csv"
    val fee_data_path = data_path_base + "/" + thisMonthFilePath_today + "/" + thisMonthFilePath_today + "SFZDATA.csv"

    val speed_base_source = sparkContext
      .textFile(speed_base_path)
      .map(x => (x.split(",")))
      .filter(x => x.length == 8)
      .filter(x => (x(6).length > 0 && x(7).length > 0))
      .map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6).toDouble, x(7).toDouble))

    val speed_data_source = sparkContext
      .textFile(speed_data_path)
      .map(x => (x.split(",")))
      .filter(x => x.length == 5)
      .filter(x => (x(0).length > 0 && x(1).length > 0 && x(2).length > 0 && x(3).length > 0 && x(4).length > 0))
      .map(x => {
        val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        val timeParse =
          try {
            DateTime.parse(x(2), fmt)
          } catch {
            case _: Throwable => DateTime.parse("1970-01-01 00:00:00", fmt)
          }
        (x(0), x(1), timeParse.getMillis, timeParse.getHourOfDay, timeParse.getDayOfMonth, timeParse.getMonthOfYear, timeParse.getYear, x(3).toInt, x(4).toInt)
      })
      .filter(x => x._3 > 0)

    val fee_data_source = sparkContext
      .textFile(fee_data_path)
      .map(x => (x.split(",")))
      .filter(x => x.length == 8)
      .filter(x => (x(1).length > 0 && x(3).length > 0))
      .map(x => {
        val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        val exTimeParse =
          try {
            DateTime.parse(x(1), fmt)
          } catch {
            case _: Throwable => DateTime.parse("1970-01-01 00:00:00", fmt)
          }

        val enTimeParse =
          try {
            DateTime.parse(x(3), fmt)
          } catch {
            case _: Throwable => DateTime.parse("1970-01-01 00:00:00", fmt)
          }
        (x(0), exTimeParse.getMillis, x(2), enTimeParse.getMillis, x(4), x(5), x(6), x(7))
      })
      .filter(x => x._2 > 0 && x._4 > 0)

    // 构造注册数据表
    import sqlContext.implicits._
    val speed_base = speed_base_source
      .map(x => SPEED_BASE(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)).toDF()
    speed_base.registerTempTable("speed_base")
    val speed_data = speed_data_source
      .map(x => SPEED_DATA(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)).toDF()
    speed_data.registerTempTable("speed_data_" + thisMonthFilePath_today)
    val fee_data = fee_data_source
      .map(x => FEE_DATA(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)).toDF()
    fee_data.registerTempTable("fee_data_" + thisMonthFilePath_today)

    //    查询
    val speed_guid = sqlContext.sql("SELECT GDCSYBM FROM speed_base WHERE LON BETWEEN " + lon_lower + " AND " + lon_upper + " AND LAT BETWEEN " + lat_lower + " AND " + lat_upper)
    speed_guid.registerTempTable("speed_guid")
    sqlContext.sql("cache table speed_guid").count()

    val speed_info_today = sqlContext.sql("select SITE_GUID, HPHM, WZSJMillis, CLSD, WZSJHourOfDay from speed_data_"+thisMonthFilePath_today+" where WZSJDayOfMonth = " + day + " and WZSMonthOfYear = " + month + " and WZSJYear = " + year)
    speed_info_today.registerTempTable("speed_info_today")

    val speed_today = sqlContext.sql("select speed_info_today.SITE_GUID as guid, speed_info_today.HPHM as plate, speed_info_today.WZSJMillis as time, speed_info_today.CLSD as speed, speed_info_today.WZSJHourOfDay as hour from speed_guid JOIN speed_info_today ON speed_guid.GDCSYBM=speed_info_today.SITE_GUID")
    speed_today.registerTempTable("speed_today")

    //    val speed_today_class_time = sqlContext.sql("select speed_today.speed as speed, speed_today.hour as hour, fee_data.EXVEHCLASS as class, fee_data.EXTRUCKFLAG as type from speed_today join fee_data on speed_today.plate=fee_data.ENVEHPLATE or speed_today.plate=fee_data.EXVEHPLATE where speed_today.time between fee_data.ENTIMEMillis and fee_data.EXTIMEMillis")
    val speed_today_class_time = sqlContext.sql("select speed_today.speed as speed, speed_today.hour as hour, fee_data_"+thisMonthFilePath_today+".EXVEHCLASS as class, fee_data_"+thisMonthFilePath_today+".EXTRUCKFLAG as type from speed_today join fee_data_"+thisMonthFilePath_today+" on speed_today.plate=fee_data_"+thisMonthFilePath_today+".ENVEHPLATE where speed_today.time between fee_data_"+thisMonthFilePath_today+".ENTIMEMillis and fee_data_"+thisMonthFilePath_today+".EXTIMEMillis")
    speed_today_class_time.registerTempTable("speed_today_class_time")
    sqlContext.sql("cache table speed_today_class_time").count()

    val speed_today_small_bus = sqlContext.sql("select 1 as time_point, \"01\" as car_type, hour as time_period, AVG(speed) as avg_carspeed from speed_today_class_time where class=1 and type=0 group by hour").toJSON.collect.toList
    val speed_today_big_bus = sqlContext.sql("select 1 as time_point, \"02\" as car_type, hour as time_period, AVG(speed) as avg_carspeed from speed_today_class_time where class>1 and type=0 group by hour").toJSON.collect.toList
    val speed_today_small_truck = sqlContext.sql("select 1 as time_point, \"03\" as car_type, hour as time_period, AVG(speed) as avg_carspeed from speed_today_class_time where class=1 and type=1 group by hour").toJSON.collect.toList
    val speed_today_big_truck = sqlContext.sql("select 1 as time_point, \"04\" as car_type, hour as time_period, AVG(speed) as avg_carspeed from speed_today_class_time where class>1 and type=1 group by hour").toJSON.collect.toList

    sqlContext.sql("uncache table speed_today_class_time").count()

    var sqlStatement = ""
    if (start.getYear() * 100 + start.getMonthOfYear() < 201606) {
      start = new DateTime(2016, 6, 1, 0, 0, 0)
    } else if (end.getYear() * 100 + end.getMonthOfYear() < 201606) {
      println()
      return List[String]().asJava
    }
    var i = new DateTime(start.getYear, start.getMonthOfYear, start.getDayOfMonth, 0, 0, 0)
    while (!(i.getYear == end.plusMonths(1).getYear && i.getMonthOfYear == end.plusMonths(1).getMonthOfYear)) {
      val thisMonthFilePath = (i.getYear*100 + i.getMonthOfYear).toString

      println("begin: " + thisMonthFilePath)

      val speed_data_path = data_path_base + "/" + thisMonthFilePath + "/" + thisMonthFilePath + "CSYDATA.csv"
      val fee_data_path = data_path_base + "/" + thisMonthFilePath + "/" + thisMonthFilePath + "SFZDATA.csv"

      val startTime = {
        if (i.getYear == start.getYear && i.getMonthOfYear == start.getMonthOfYear) {
          start.getMillis
        } else {
          sqlStatement += " union "
          (new DateTime(i.getYear, i.getMonthOfYear, 1, 0, 0, 0)).getMillis
        }
      }
      val endTime = {
        if (i.getYear == end.getYear && i.getMonthOfYear == end.getMonthOfYear) {
          end.plusDays(1).getMillis
        } else {
          (new DateTime(i.getYear, i.getMonthOfYear, 1, 0, 0, 0)).plusMonths(1).getMillis
        }
      }

      val speed_data_source = sparkContext
        .textFile(speed_data_path)
        .map(x => (x.split(",")))
        .filter(x => x.length == 5)
        .filter(x => (x(0).length > 0 && x(1).length > 0 && x(2).length > 0 && x(3).length > 0 && x(4).length > 0))
        .map(x => {
          val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
          val timeParse =
            try {
              DateTime.parse(x(2), fmt)
            } catch {
              case _ : Throwable => DateTime.parse("1970-01-01 00:00:00", fmt)
            }
          (x(0), x(1), timeParse.getMillis, timeParse.getHourOfDay, timeParse.getDayOfMonth, timeParse.getMonthOfYear, timeParse.getYear, x(3).toInt, x(4).toInt)
        })
        .filter(x => x._3 > 0)

      val fee_data_source = sparkContext
        .textFile(fee_data_path)
        .map(x => (x.split(",")))
        .filter(x => x.length == 8)
        .filter(x => (x(1).length > 0 && x(3).length > 0))
        .map(x => {
          val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
          val exTimeParse =
            try {
              DateTime.parse(x(1), fmt)
            } catch {
              case _ : Throwable => DateTime.parse("1970-01-01 00:00:00", fmt)
            }

          val enTimeParse =
            try {
              DateTime.parse(x(3), fmt)
            } catch {
              case _ : Throwable => DateTime.parse("1970-01-01 00:00:00", fmt)
            }
          (x(0), exTimeParse.getMillis, x(2), enTimeParse.getMillis, x(4), x(5), x(6), x(7))
        })
        .filter(x => x._2 > 0 && x._4 > 0)

      // 构造注册数据表
      import sqlContext.implicits._
      val speed_data = speed_data_source
        .map(x => SPEED_DATA(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)).toDF()
      speed_data.registerTempTable("speed_data_"+thisMonthFilePath)
      val fee_data = fee_data_source
        .map(x => FEE_DATA(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)).toDF()
      fee_data.registerTempTable("fee_data_"+thisMonthFilePath)

      //    查询
      val speed_info_previous = sqlContext.sql("select SITE_GUID, HPHM, WZSJMillis, CLSD, WZSJHourOfDay from speed_data_"+thisMonthFilePath+" where WZSJMillis >= " + startTime + " and WZSJMillis < " + endTime)
      speed_info_previous.registerTempTable("speed_info_previous_"+thisMonthFilePath)

      val speed_previous = sqlContext.sql("select speed_info_previous_"+thisMonthFilePath+".SITE_GUID as guid, speed_info_previous_"+thisMonthFilePath+".HPHM as plate, speed_info_previous_"+thisMonthFilePath+".WZSJMillis as time, speed_info_previous_"+thisMonthFilePath+".CLSD as speed, speed_info_previous_"+thisMonthFilePath+".WZSJHourOfDay as hour from speed_guid join speed_info_previous_"+thisMonthFilePath+" on speed_guid.GDCSYBM=speed_info_previous_"+thisMonthFilePath+".SITE_GUID")
      speed_previous.registerTempTable("speed_previous_"+thisMonthFilePath)

      //    val speed_previous_class_time = sqlContext.sql("select speed_previous.speed as speed, speed_previous.hour as hour, fee_data.EXVEHCLASS as class, fee_data.EXTRUCKFLAG as type from speed_previous join fee_data on speed_previous.plate=fee_data.ENVEHPLATE or speed_previous.plate=fee_data.EXVEHPLATE where speed_previous.time between fee_data.ENTIMEMillis and fee_data.EXTIMEMillis")
      val speed_previous_class_time = sqlContext.sql("select speed_previous_"+thisMonthFilePath+".speed as speed, speed_previous_"+thisMonthFilePath+".hour as hour, fee_data_"+thisMonthFilePath+".EXVEHCLASS as class, fee_data_"+thisMonthFilePath+".EXTRUCKFLAG as type from speed_previous_"+thisMonthFilePath+" join fee_data_"+thisMonthFilePath+" on speed_previous_"+thisMonthFilePath+".plate=fee_data_"+thisMonthFilePath+".ENVEHPLATE where speed_previous_"+thisMonthFilePath+".time between fee_data_"+thisMonthFilePath+".ENTIMEMillis and fee_data_"+thisMonthFilePath+".EXTIMEMillis")
      speed_previous_class_time.registerTempTable("speed_previous_class_time_"+thisMonthFilePath)
      sqlContext.sql("cache table speed_previous_class_time_"+thisMonthFilePath).count()
      sqlStatement += " select * from speed_previous_class_time_" + thisMonthFilePath + " "

      i = i.plusMonths(1)
    }
    println(sqlStatement)

    val speed_previous_small_bus = sqlContext.sql("select 0 as time_point, \"01\" as car_type, S.hour as time_period, AVG(S.speed) as avg_carspeed from (" + sqlStatement + ") S where S.class=1 and S.type=0 group by S.hour").toJSON.collect().toList
    val speed_previous_big_bus = sqlContext.sql("select 0 as time_point, \"02\" as car_type, S.hour as time_period, AVG(S.speed) as avg_carspeed from (" + sqlStatement + ") S where S.class>1 and S.type=0 group by S.hour").toJSON.collect().toList
    val speed_previous_small_truck = sqlContext.sql("select 0 as time_point, \"03\" as car_type, S.hour as time_period, AVG(S.speed) as avg_carspeed from (" + sqlStatement + ") S where S.class=1 and S.type=1 group by S.hour").toJSON.collect().toList
    val speed_previous_big_truck = sqlContext.sql("select 0 as time_point, \"04\" as car_type, S.hour as time_period, AVG(S.speed) as avg_carspeed from (" + sqlStatement + ") S where S.class>1 and S.type=1 group by S.hour").toJSON.collect().toList

    return (speed_previous_small_bus ::: speed_previous_big_bus ::: speed_previous_small_truck ::: speed_previous_big_truck ::: speed_today_small_bus ::: speed_today_big_bus ::: speed_today_small_truck ::: speed_today_big_truck).asJava

  }
}