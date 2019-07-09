package com.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

// import 문 생략
object s1_dataLoadingFile {
  def main(args: Array[String]): Unit = {
    // Spark 세션 생성 (1)
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기 (2)
    var missingValueFile = "missingvalue.csv"

    // 절대경로 입력
    var missingValueData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+missingValueFile)

    // 데이터 확인 (3)
    print(missingValueData.show)


    //3.정제 when, otherwise 도 있음
    // 절대경로 입력 - promotionData.csv 파일을 이용할게요
    var promotionData=
    spark.read.format("csv").
      option("header","true").
      option("Delimiter",",").
      load("c:/spark/bin/data/promotionData.csv")
    // 데이터 확인
    print(promotionData.show)

    // 컬럼간 값 비교 후 처리 -withColumn 은 ("새로 만들 컬럼명", 담을 값)
//    var promotionDataRefining = promotionData.withColumn("NEW_DISCOUNT",
//      when($"DISCOUNT" > $"PRICE" , 0).
//        otherwise($"DISCOUNT"))
//    => 얘는 컬럼값이 string 이어서 "300" > "1000" 보다 크다고 인식. 왜냐면 3이 더 크니깐
//        그래서 아래에서 형변환해줘서 이런 문제 없게 만듬


    var promotionDataRefining = promotionData.withColumn("NEW_DISCOUNT",
                              when($"DISCOUNT".cast("Double") > $"PRICE".cast("Double") , 0).
                                otherwise($"DISCOUNT"))


    // 새컬럼 만들어서 타입 변환한 값 넣기
    var refinedData = selloutData.map(x=>{
      var maxValue = 150000
      var volume = x.getString(3).toDouble
      if( volume >maxValue) { volume = 150000}
      (x.getString(0), x.getString(1), x.getString(2), volume)})

    var selloutData2= selloutData.withColumn("NEW_VOLUME", $"VOLUME".cast("Double"))


    //4.정렬
    //orderBy, sort 차이
    //sort 는 자동으로 내림차순
    //orderBy 는 내림차순, 오름차순 설정할 수 있어서 이걸 많이 씀!
    import org.apache.spark.sql.functions._

    var sortedDf = selloutData.sort("YEARWEEK","PRODUCTGROUP")
    var orderedDf = selloutData.orderBy($"VOLUME".desc, $"YEARWEEK".asc)



























  }
}
