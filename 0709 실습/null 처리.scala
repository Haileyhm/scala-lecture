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


    //missingValue, null 값 처리

    //1.조회
    //null 인 컬럼 조회 1) x.getString(x) == null
    //rdd 에서도 쓸 수 있는 명령어야 -> 근데 이거 쓰면 dataset 으로 자료형이 바뀌어 dataframe - dateset - rdd ,
    missingValueData.filter(x=>{x.get(4).toString.toDouble>2000})

    //x.get(4).toString == "" 가 안된다 _ null 에 함수를 적용하면서 에러가 생기기 때문
    //x.getString(4).toDouble 도 마찬가지
    //spark 컬럼 인덱스는 0부터 시작.
    var missingValueNull = missingValueData.filter(x=>{x.getString(1) == null })


    //null 인 컬럼 조회 2) isNull 은 rdd 에서는 못 씀
    var missingValueDfNull = missingValueData.filter(($"PRODUCTGROUP".isNull) || ($"TARGET".isNull))


    //2. 채우기
    // 전체컬럼 대상 null 채우기
    var missingValueFillNull = missingValueData.na.fill("0")

    // 특정컬럼 대상 null 채우기
    // var TargetCol = ("VOLUME" , "TARGET") -- 이건 안됨
    var TargetCol = Array("VOLUME", "TARGET")
    var missingValueFillTargetCol = missingValueData.na.fill("0", TargetCol)


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
    var promotionDataRefining = promotionData.withColumn("NEW_DISCOUNT",
                              when($"DISCOUNT" > $"PRICE" , 0).
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
    import org.apache.spark.sql.functions._

    var sortedDf = selloutData.sort("YEARWEEK","PRODUCTGROUP")
    var orderedDf = selloutData.orderBy($"VOLUME".desc, $"YEARWEEK".asc)


    //5.




























  }
}
