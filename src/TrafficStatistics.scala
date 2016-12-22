package org.dbgroup.trafficbigdata
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.dbgroup.trafficbigdata.overspeedcount.OverSpeedCount
import org.dbgroup.trafficbigdata.avgspeed.AverageSpeed
import org.dbgroup.trafficbigdata.accident.AccidentStatistics
import collection.JavaConverters._
/**
  * Created by sunji on 16/12/21.
  */
class TrafficStatistics {

  var sparkContext: SparkContext = null
  var sqlContext: SQLContext = null
  var data_path_base = ""

  def init(path: String, args: java.util.Map[String, String], master: String = "local"): Unit = {
    var sparkConf = new SparkConf().setAppName("TrafficBigData").setMaster(master) // .set("spark.sql.shuffle.partitions", "2")
    args.asScala.foreach(x => (sparkConf = sparkConf.set(x._1, x._2)))
    sparkContext = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sparkContext)
    data_path_base = path
  }

  def getOverSpeedCount(lon_upper: Double, lon_lower: Double, lat_upper: Double, lat_lower: Double, start_date: String, end_date: String): java.util.List[String] = {
    OverSpeedCount.getOverSpeedCount(sparkContext, sqlContext, data_path_base, lon_upper, lon_lower, lat_upper, lat_lower, start_date, end_date)
  }

  def getAverageSpeed(lon_upper: Double, lon_lower: Double, lat_upper: Double, lat_lower: Double, date: String): java.util.List[String] = {
    AverageSpeed.getAverageSpeed(sparkContext, sqlContext, data_path_base, lon_upper, lon_lower, lat_upper, lat_lower, date)
  }

  def getAccidentCount(lon_upper: Double, lon_lower: Double, lat_lower: Double, lat_upper: Double, start_date: String, end_date: String): java.util.List[String] = {
    AccidentStatistics.getAccidentCount(sparkContext, sqlContext, data_path_base, lon_upper, lon_lower, lat_lower, lat_upper, start_date, end_date)
  }

  def destroy(): Unit = {
    sparkContext.stop()
    sqlContext.clearCache()
  }
}
