package org.dbgroup.trafficbigdata.overspeedcount

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
case class SPEED_DATA(SITE_GUID: String, HPHM: String, WZSJMillis: Long, WZSJHourOfDay: Int, CLSD: Int, ISOVERSPEED: Int)
case class FEE_DATA(EXSTATION: String, EXTIMEMillis: Long, ENSTATION: String, ENTIMEMillis: Long, EXVEHCLASS: String, ENVEHPLATE: String, EXVEHPLATE: String, EXTRUCKFLAG: String)

object OverSpeedCount {

  def getOverSpeedCount(sparkContext: SparkContext, sqlContext: SQLContext, data_path_base: String, lon_upper: Double, lon_lower: Double, lat_upper: Double, lat_lower: Double, start_date: String, end_date: String): java.util.List[String] = {

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val start = DateTime.parse(start_date, fmt)
    val end = DateTime.parse(end_date, fmt)
    // 数据预处理

    val speed_base_path = data_path_base + "/speed_base.csv"
    val speed_base_source = sparkContext
      .textFile(speed_base_path)
      .map(x => (x.split(",")))
      .filter(x => x.length == 8)
      .filter(x => (x(6).length > 0 && x(7).length > 0))
      .map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6).toDouble, x(7).toDouble))
    import sqlContext.implicits._
    val speed_base = speed_base_source
      .map(x => SPEED_BASE(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)).toDF()
    speed_base.registerTempTable("speed_base")
    val speed_guid = sqlContext.sql("SELECT GDCSYBM FROM speed_base WHERE LON BETWEEN "+lon_lower+" AND "+lon_upper+" AND LAT BETWEEN " +lat_lower+ " AND "+lat_upper)
    speed_guid.registerTempTable("speed_guid")
    sqlContext.sql("cache table speed_guid").count()

    var sqlStatement = ""
    var i = DateTime.parse(start_date, fmt)
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
          (x(0), x(1), timeParse.getMillis, timeParse.getHourOfDay, x(3).toInt, x(4).toInt)
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
        .map(x => SPEED_DATA(x._1, x._2, x._3, x._4, x._5, x._6)).toDF()
      speed_data.registerTempTable("speed_data_"+thisMonthFilePath)
      val fee_data = fee_data_source
        .map(x => FEE_DATA(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)).toDF()
      fee_data.registerTempTable("fee_data_"+thisMonthFilePath)

      // 查询
      val speed_info = sqlContext.sql("select SITE_GUID, HPHM, WZSJMillis, CLSD, WZSJHourOfDay from speed_data_"+thisMonthFilePath+" where WZSJMillis BETWEEN " + startTime + " AND " + endTime)
      speed_info.registerTempTable("speed_info_" + thisMonthFilePath)
      val speed = sqlContext.sql("select speed_info_"+thisMonthFilePath+".SITE_GUID as guid, speed_info_"+thisMonthFilePath+".HPHM as plate, speed_info_"+thisMonthFilePath+".WZSJMillis as time, speed_info_"+thisMonthFilePath+".CLSD as speed, speed_info_"+thisMonthFilePath+".WZSJHourOfDay as hour from speed_guid JOIN speed_info_"+thisMonthFilePath+" ON speed_guid.GDCSYBM=speed_info_"+thisMonthFilePath+".SITE_GUID")
      speed.registerTempTable("speed_" + thisMonthFilePath)

      //    val speed_class_time = sqlContext.sql("select speed.speed as speed, speed.hour as hour, fee_data.EXVEHCLASS as class, fee_data.EXTRUCKFLAG as type from speed join fee_data on speed.plate=fee_data.ENVEHPLATE or speed.plate=fee_data.EXVEHPLATE where speed.time between fee_data.ENTIMEMillis and fee_data.EXTIMEMillis")
      val speed_class_time = sqlContext.sql("select speed_"+thisMonthFilePath+".speed as speed, speed_"+thisMonthFilePath+".hour as hour, fee_data_"+thisMonthFilePath+".EXVEHCLASS as class, fee_data_"+thisMonthFilePath+".EXTRUCKFLAG as type from speed_"+thisMonthFilePath+" join fee_data_"+thisMonthFilePath+" on speed_"+thisMonthFilePath+".plate=fee_data_"+thisMonthFilePath+".ENVEHPLATE where speed_"+thisMonthFilePath+".time between fee_data_"+thisMonthFilePath+".ENTIMEMillis and fee_data_"+thisMonthFilePath+".EXTIMEMillis")
      speed_class_time.registerTempTable("speed_class_time_" + thisMonthFilePath)
      sqlContext.sql("cache table speed_class_time_" + thisMonthFilePath).count()
      sqlStatement += " select * from speed_class_time_" + thisMonthFilePath + " "

      i = i.plusMonths(1)
    }

    println(sqlStatement)

    val overSpeedCount1 = sqlContext.sql("select \"01\" as car_type, S.hour as time_period, COUNT(*) as speed_limit_num from (" + sqlStatement + ") S where S.class=1 and S.type=0 and S.speed > 120 group by S.hour").toJSON.collect().toList
    val overSpeedCount2 = sqlContext.sql("select \"02\" as car_type, S.hour as time_period, COUNT(*) as speed_limit_num from (" + sqlStatement + ") S where S.class>1 and S.type=0 and S.speed > 120 group by S.hour").toJSON.collect().toList
    val overSpeedCount3 = sqlContext.sql("select \"03\" as car_type, S.hour as time_period, COUNT(*) as speed_limit_num from (" + sqlStatement + ") S where S.class=1 and S.type=1 and S.speed > 120 group by S.hour").toJSON.collect().toList
    val overSpeedCount4 = sqlContext.sql("select \"04\" as car_type, S.hour as time_period, COUNT(*) as speed_limit_num from (" + sqlStatement + ") S where S.class>1 and S.type=1 and S.speed > 100 group by S.hour").toJSON.collect().toList

    return (overSpeedCount1 ::: overSpeedCount2 ::: overSpeedCount3 ::: overSpeedCount4).asJava
  }
}