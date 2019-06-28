package com.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s8_dataWritingDatabase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._


    //QUIZ2_PRO_ACTUAL_SALES의 데이터를 살펴보니... 중간중간에 데이터가 빠져있다 데이터의 빈 공간에 실적 0이라고 채우자!
    ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
    // 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기 (2)
    var paramFile = "PRO_ACTUAL_SALES.CSV"

    // 절대경로 입력
    var paramData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+paramFile)

    // 데이터 확인 (3)
    print(paramData.show)


    //데이터 정제- 빠진 YEARWEEK 파악
    //step1. groupBy 지역, 상품
    //step2. data에서 qty만 뽑아서 정제
    //step3. 111 에 저장

    //step1
    //1) 컬럼별 인데스 생성
//    var rddColumns = paramData.columns.map(x=>{
//      x.toUpperCase
//    })

    var regionSeg1No = rddColumns.indexOf("regionSeg1")
    var productSeg2No = rddColumns.indexOf("productSeg2")
    var regionSeg2No = rddColumns.indexOf("regionSeg2")
    var regionSeg3No = rddColumns.indexOf("regionSeg3")
    var productSeg3No = rddColumns.indexOf("productSeg3")
    var yearweekNo = rddColumns.indexOf("yearweek")
    var yearNo = rddColumns.indexOf("year")
    var weekNo = rddColumns.indexOf("week")
    var qtyNo = rddColumns.indexOf("qty")


    //2) RDD 변환
    var quiz2Rdd = paramData.rdd

    //step2
    //1) 지역, 상품별 평균 거래량 산출
    //REGIONID, PRODUCT, YEARWEEK, QTY
    var quiz1GroupRdd = quiz1Rdd.groupBy(x=>{
      (x.getString(regionidNo), x.getString(productNo))
    }).map(x=> {
      // 그룹별 분산처리가 수행됨
      var key = x._1
      var data = x._2

      //var qtySum = data.map(x=>{x.getString(qtyNo).toDouble}).sum
      //BigDecimal cannot be cast to java.lang.String 오류가 남
      //qtyNo 이 number 타입인데 String으로 변환할 때 나는 오류 -> 아래처럼 변환
      var qtySum = data.map(x=>{String.valueOf(x.get(qtyNo)).toDouble}).sum
      (x._1, x._2, qtySum) // (key, qtySum) key들을 array처럼 인식해서 오류가 남
    })


    //2) Rdd를 데이터프레임으로 변환
    var middleResult = quiz1GroupRdd.toDF("REGIONID","PRODUCT","QTY")
    println(middleResult.show)

    //3) 데이터베이스 주소 및 접속정보 설정
    var outputUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var outputUser = "SYSTEM"
    var outputPw = "manager"

    //4) 데이터 저장
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)

    //3) tempView 에 올림
    middleResult.createOrReplaceTempView("Haeri_MIDDLETABLE")

    val table = "KOPO_2019_HAERI"
    //append
    middleResult.write.mode("overwrite").jdbc(outputUrl, table, prop)
    res90.write.mode("overwrite").jdbc(outputUrl, table, prop)

  }
}
