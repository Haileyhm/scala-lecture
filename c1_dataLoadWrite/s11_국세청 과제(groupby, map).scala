package com.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.reflect.internal.ModifierFlags

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
    var joinDataFile1 = "joindata1.csv"
    var joinDataFile2 = "joindata2.csv"
    // 절대경로 입력
    var joinData1=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+joinDataFile1)


    var joinData2=
      spark.read.format("csv").
        option("header","true").
        option("encoding","ms949").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+joinDataFile2)

    // 데이터 확인 (3)
    print(joinData1.show)
    print(joinData2.show)

    //컬럼 인덱싱
    var joinDataCol1 = joinData1.columns
    var joinDataCol2 = joinData2.columns

    var join1Col1 = joinDataCol1.indexOf("mata_tin")
    var join1Col2 = joinDataCol1.indexOf("mata_txpr_sex_cd")
    var join1Col3 = joinDataCol1.indexOf("mate_mdf_yn")
    var join1Col4 = joinDataCol1.indexOf("mate_srcs_cntn")
    var join1Col5 = joinDataCol1.indexOf("mem_rit_cd")
    var join1Col6 = joinDataCol1.indexOf("rit_end_dt")
    var join1Col7 = joinDataCol1.indexOf("rit_strt_dt")
    var join1Col8 = joinDataCol1.indexOf("suta_tin")
    var join1Col9 = joinDataCol1.indexOf("suta_txpr_sex_cd")

    var join2Col1 = joinDataCol2.indexOf("mata_tin")
    var join2Col2 = joinDataCol2.indexOf("mate_srcs_cntn")
    var join2Col3 = joinDataCol2.indexOf("rit_end_dt")
    var join2Col4 = joinDataCol2.indexOf("rit_strt_dt")
    var join2Col5 = joinDataCol2.indexOf("suta_tin")
    var join2Col6 = joinDataCol2.indexOf("txpr_rlt_cd")

    // Rdd로 변환
    var joinData1Rdd = joinData1.rdd
    var joinData2Rdd = joinData2.rdd

    //같은지 조회 후 컬럼에 넣기
    var joiningData = joinData2Rdd.map(x=>{
     x.getString(join2Col5)
    })

    var joinedData = joinData1Rdd.map(x=>{
      var new_sutatin = ""
      if(x.getString(join1Col8) == joiningData){ //if가 안 먹네
        new_sutatin = x.getString(join1Col1)
      }else{
        new_sutatin = "x"
      }
      (x.getString(join1Col1),
        x.getString(join1Col2),
        x.getString(join1Col3),
        x.getString(join1Col4),
        x.getString(join1Col5),
        x.getString(join1Col6),
        x.getString(join1Col7),
        x.getString(join1Col8),
        x.getString(join1Col9),
        new_sutatin)
    })

    //답보고 다시 해본 거
    var joiningData = joinData2Rdd.map(x=>{
      x.getString(join2Col5)
    })

    var joinedData = joinData1Rdd.map(x=>{
      var new_sutatin = ""
      if(x.getString(join1Col8).contains(joiningData)){
        new_sutatin = x.getString(join1Col1)
      }else{
        new_sutatin = "x"
      }
      (x.getString(join1Col1),
        x.getString(join1Col2),
        x.getString(join1Col3),
        x.getString(join1Col4),
        x.getString(join1Col5),
        x.getString(join1Col6),
        x.getString(join1Col7),
        x.getString(join1Col8),
        new_sutatin)
    })

    //시도2
    var joinedData = joinData1Rdd.map(x=>{
      var new_sutatin = ""
      joinData2Rdd.filter(x=>{
        if(x.getString(join1Col8) == joiningData){
          new_sutatin = x.getString(join1Col1)
        }else{
          new_sutatin = "x"
        }
      })
      (x.getString(join1Col1),
        x.getString(join1Col2),
        x.getString(join1Col3),
        x.getString(join1Col4),
        x.getString(join1Col5),
        x.getString(join1Col6),
        x.getString(join1Col7),
        x.getString(join1Col8),
        new_sutatin)
    })

    var joinedData = joinData1Rdd.map(x=>{
      var new_sutatin = ""
      joinData2Rdd.map(row=>{
        if(x.getString(join1Col8) == row.getString(join2Col5)){
           new_sutatin = x.getString(join1Col1)
        }else{
          new_sutatin = "x"
        }
      })
      (x.getString(join1Col1),
        x.getString(join1Col2),
        x.getString(join1Col3),
        x.getString(join1Col4),
        x.getString(join1Col5),
        x.getString(join1Col6),
        x.getString(join1Col7),
        x.getString(join1Col8),
        new_sutatin)
    })

    //쿼리로 하기 1
    SELECT A.*, B.*
    FROM JOINTABLE1 A
    LEFT JOIN JOINTABLE2 B
    ON A.SUTA_TIN = B.SUTA_TIN

    //쿼리로 하기 2
    SELECT
      CASE WHEN B.SUTA_TIN IS NULL THEN A.SUTA_TIN
      ELSE B.MATA_TIN END AS UPDATED_SUTA_TIN,
      A.*, B.*
    FROM JOINTABLE1 A
    LEFT JOIN JOINTABLE2 B
    ON A.SUTA_TIN = B.SUTA_TIN

    //쿼리로 하기 3
    SELECT UPDATED_SUTA_TIN-UP2 AS DIFF
    FROM(
      SELECT
        CASE WHEN B.SUTA_TIN IS NULL THEN A.SUTA_TIN
        ELSE B.MATA_TIN END AS UPDATED_SUTA_TIN,
        NVL2(B.SUTA_TIN, B.MATA_TIN)
      A.*, B.*
    FROM JOINTABLE1 A
    LEFT JOIN JOINTABLE2 B
    ON A.SUTA_TIN = B.SUTA_TIN)
  WHERE UPDATE_SUTA_TIN-UP2 != 0


    var staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/xe"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb1 = "JOINTABLE1"

    // 관계형 데이터베이스 Oracle 연결 (2)
    val dataFromOracle= spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb1).
      option("user",staticUser).
      option("password",staticPw).load



    //답=============================================
    //컬럼 소문자 변환
    var join2DfColumns = joinData2.columns.map(x=>{ x.toLowerCase())})

    //컬럼 인덱스 정의
    var sutatinNo =  join2DfColumns.indexOf("suta_tin")
    var matatinNo =  join2DfColumns.indexOf("mata_tin")

    //key, value 형태로 묶음
    joinData2.map(x=>{
      (x.getString(sutatinNo), //key
        x.getString(matatinNo))  //value
    })

    //collectAsMap 을 쓰는 건 groupby를 껴넣는다
  var join2Rdd = joinData2.rdd

  var join2Map.groupBy(x=>{
    (x.getString(sutatinNo))
  }).map{x=>

      var key = x._1.trim()
      var data = x._2
      var matatinValue = data.map(x=>{x.getString(matatinNo).trim()})
      (key, matatinValue)
  }
})


  //답2
  var answer = joinData1Rdd.map
  var new_sutatin = ""




  }
}


