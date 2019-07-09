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

    // 절대경로 입력 - promotionData.csv 파일을 이용할게요
    var promotionData=
    spark.read.format("csv").
      option("header","true").
      option("Delimiter",",").
      load("c:/spark/bin/data/promotionData.csv")
    // 데이터 확인
    print(promotionData.show)

   //5. 집계함수
   var groupDf = selloutData.
     groupBy($"REGIONID", $"PRODUCTGROUP").
    agg(mean($"VOLUME") as "MEAN_VOLUME")

























  }
}
