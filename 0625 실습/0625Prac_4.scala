//동준이 코드
package com.kdj

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object SparkUseTest4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val salesFile = "pro_actual_sales2.csv"
    val salesData = spark.read.format("csv").
      option("header", "true").
      option("Delimiter", ",").
      load("c:/spark/bin/data/" + salesFile)

    val salesColumns = salesData.columns
    val noRegionSeg1 = salesColumns.indexOf("regionSeg1")
    val noProductSeg1 = salesColumns.indexOf("productSeg1")
    val noProductSeg2 = salesColumns.indexOf("productSeg2")
    val noRegionSeg2 = salesColumns.indexOf("regionSeg2")
    val noRegionSeg3 = salesColumns.indexOf("regionSeg3")
    val noProductSeg3 = salesColumns.indexOf("productSeg3")
    val noYearweek = salesColumns.indexOf("yearweek")
    val noYear = salesColumns.indexOf("year")
    val noWeek = salesColumns.indexOf("week")
    val noQty = salesColumns.indexOf("qty")

    val salesRdd = salesData.rdd

    val maxYearweek = salesRdd.map(x=>{ //rdd 에 있는 yearweek 의 max 값을 구함
      x.getString(noYearweek).toInt
    }).max()

    val recent20Yearweek = preWeek(maxYearweek, 20).toInt
    val recent20Data = salesRdd.filter(x=>{
      val yearweek = x.getString(noYearweek).toInt
      if (yearweek >= recent20Yearweek) {
        true
      } else {
        false
      }
    })

    val resultFlatMap = recent20Data.groupBy(x=>{
      (
        x.getString(noRegionSeg1),
        x.getString(noProductSeg2),
        x.getString(noProductSeg3)
      )
    }).flatMap(x=>{

      val data = x._2

      val dataArray: Array[Int] = data.map(x=>{
        x.get(noQty).toString.toInt
      }).toArray.sorted

      val lowerRank30Index: Int = Math.ceil(0.3 * dataArray.length).toInt - 1
      val lowerRank70Index: Int = Math.ceil(0.7 * dataArray.length).toInt - 1
      val lowerRank30Value: Int = dataArray(lowerRank30Index)
      val lowerRank70Value: Int = dataArray(lowerRank70Index)

      val finalMapResult = data.map(x=>{

        val qty: Int = x.get(noQty).toString.toInt
        var cluster: String = "Cluster 0"
        if (qty < lowerRank30Value) {
          cluster = "Cluster 2"
        } else if (qty < lowerRank70Value) {
          cluster = "Cluster 1"
        }

        Row(
          x.getString(noRegionSeg1),
          x.getString(noProductSeg1),
          x.getString(noProductSeg2),
          x.getString(noRegionSeg2),
          x.getString(noRegionSeg3),
          x.getString(noProductSeg3),
          x.getString(noYearweek),
          x.getString(noYear),
          x.getString(noWeek),
          x.getString(noQty).toInt,
          cluster
        )
      })
      finalMapResult
    })

    resultFlatMap.collect.foreach(println)

    val groupMap = resultFlatMap.groupBy(x=>{
      (
        x.getString(noRegionSeg1),
        x.getString(noProductSeg2),
        x.getString(noProductSeg3)
      )
    }).collectAsMap

    var key: (String, String, String) = ("A01", "PG05", "ITEM0410")
    findCluster(groupMap, key)
  }

  def findCluster(groupMap: scala.collection.Map[(String, String, String), Iterable[org.apache.spark.sql.Row]], key: (String, String, String)): Array[org.apache.spark.sql.Row] = {
    if (groupMap.contains(key)) {
      return groupMap(key).toArray
    } else {
      return Array.empty
    }
  }

  def getLastWeek(year: Int): Int = {
    val calendar: java.util.Calendar = Calendar.getInstance()

    calendar.setMinimalDaysInFirstWeek(4)
    calendar.setFirstDayOfWeek(Calendar.MONDAY)
    val dateFormat: java.text.SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    calendar.setTime(dateFormat.parse(year + "1231"))

    return calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
  }

  def preWeek(inputYearWeek: Any, gapWeek: Any): String = {

    var strInputYearWeek: String = inputYearWeek.toString
    var strGapWeek: String = gapWeek.toString

    var inputYear: Int = Integer.parseInt(strInputYearWeek.substring(0, 4))
    var inputWeek: Int = Integer.parseInt(strInputYearWeek.substring(4))
    var inputGapWeek: Int = Integer.parseInt(strGapWeek)

    var calcWeek: Int = inputWeek - inputGapWeek.abs

    while(calcWeek <= 0) {
      inputYear -= 1
      calcWeek += getLastWeek(inputYear)
    }

    var resultYear: String = inputYear.toString
    var resultWeek: String = calcWeek.toString
    if (resultWeek.length < 2) {
      resultWeek = "0" + resultWeek
    }

    return resultYear + resultWeek
  }

}