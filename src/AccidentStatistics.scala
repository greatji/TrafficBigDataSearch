package org.dbgroup.trafficbigdata.accident

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time._
import org.joda.time.format._
/**
  * Created by sunji on 16/12/12.
  */
// 构造数据表schema
case class ACCIDENT(ACCIDENTCLASS: Int,
                    LOSSMONEY: Int,
                    CASEID: String,
                    CASEDATE: String,
                    CASELEVEL: Int,
                    CASELOCROADID: Int,
                    CASELOCROAD: String,
                    CASELOCORADPART: String,
                    CASELOCDIRECTION: String,
                    CASELOCKILO: String,
                    CASELOCMETER: Int,
                    CASELONGITUDE: Double,
                    CASELATITUDE: Double,
                    ACCIDENTTYPE: String,
                    DEATHNUM: Int,
                    GREVIOUSINJURYNUM: Int,
                    SLIGHTINJURYNUM: Int,
                    CRASHEDMOTORVEHICLENUM: Int,
                    LANDFORM: String,
                    WEATHER: String,
                    hour: Int)

object AccidentStatistics {

  def parseToInt(x : String): Int = {
    try {
      x.toInt
    } catch {
      case _: Throwable => 0
    }
  }

  def parseToDouble(x : String): Double = {
    try {
      x.toDouble
    } catch {
      case _: Throwable => 0.0
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TrafficBigData")// .setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    // 参数传入
    val accident_path = args(0)// "/Users/sunji/Work/tsinghua/TrafficBigData/resource/TF_ZFZD_CASESPECIFICATION.csv"
    val lon_upper = args(1).toDouble // 106.0
    val lon_lower = args(2).toDouble // 105.0
    val lat_upper = args(3).toDouble // 30.0
    val lat_lower = args(4).toDouble // 29.0

    // 数据预处理

    val accident_source = sparkContext
      .textFile(accident_path)
      .map(x => (x.split(",")))
      .filter(x => x.length == 20)
      .map(x => x.map(s => s.slice(1, s.length-1)))
      .map(x => {
        val ACCIDENTCLASS = parseToInt(x(0))
        val LOSSMONEY = parseToInt(x(1))
        val CASEID = x(2)
        val CASEDATE = x(3)
        val CASELEVEL = parseToInt(x(4))
        val CASELOCROADID = parseToInt(x(5))
        val CASELOCROAD = x(6)
        val CASELOCORADPART = x(7)
        val CASELOCDIRECTION = x(8)
        val CASELOCKILO = x(9)
        val CASELOCMETER = parseToInt(x(10))
        val CASELONGITUDE = parseToDouble(x(11))
        val CASELATITUDE = parseToDouble(x(12))
        val ACCIDENTTYPE = x(13)
        val DEATHNUM = parseToInt(x(14))
        val GREVIOUSINJURYNUM = parseToInt(x(15))
        val SLIGHTINJURYNUM = parseToInt(x(16))
        val CRASHEDMOTORVEHICLENUM = parseToInt(x(17))
        val LANDFORM = x(18)
        val WEATHER = x(19)
        val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        val timeParse =
          try {
            DateTime.parse(x(3), fmt)
          } catch {
            case _ : Throwable => DateTime.parse("1970-01-01 00:00:00", fmt)
          }
        val hour = timeParse.getHourOfDay
        ACCIDENT(ACCIDENTCLASS, LOSSMONEY, CASEID, CASEDATE, CASELEVEL, CASELOCROADID, CASELOCROAD, CASELOCORADPART, CASELOCDIRECTION, CASELOCKILO, CASELOCMETER, CASELONGITUDE, CASELATITUDE, ACCIDENTTYPE, DEATHNUM, GREVIOUSINJURYNUM, SLIGHTINJURYNUM, CRASHEDMOTORVEHICLENUM, LANDFORM, WEATHER, hour)
      })

    // 构造注册数据表
    import sqlContext.implicits._
    val accident = accident_source.toDF()
    accident.registerTempTable("accident")

    // 查询
    val accident_info = sqlContext.sql("SELECT * FROM accident WHERE CASELONGITUDE BETWEEN " + lon_lower + " AND " + lon_upper + " AND CASELATITUDE BETWEEN " + lat_lower + " AND " + lat_upper)
    accident_info.registerTempTable("accident_info")
    sqlContext.sql("cache table accident_info").collect
    val info = sqlContext.sql("SELECT * FROM accident_info").toJSON.saveAsTextFile(args(5)+"/accident_info")
    val accident_count = sqlContext.sql("SELECT COUNT(*) as accidentcount, hour FROM accident_info GROUP BY hour").toJSON.saveAsTextFile(args(5)+"/accident_count")

    println("complete!!!")
  }
}