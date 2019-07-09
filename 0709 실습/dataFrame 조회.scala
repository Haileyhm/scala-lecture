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
    var paramFile = "KOPO_PRODUCT_VOLUME.csv"

    // 절대경로 입력
    var selloutData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+paramFile)

    // 데이터 확인 (3)
    print(selloutData.show)


    //dataframe 조회하기
    //데이터프레임 행 조회
    var selloutDataRow = selloutData.filter($"REGIONID" === "A01" && $"YEARWEEK" < 201515)

    //데이터프레임 열 조회
    var selloutDataCol = selloutData.select("PRODUCTGROUP","YEARWEEK","VOLUME").
    filter($"YEARWEEK" > 201650 && $"PRODUCTGROUP" === "ST0002")

  }
}
