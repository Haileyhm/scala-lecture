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

    ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
    // 접속정보 설정 (1)
    //접속정보 맞추는 것 중요함
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "KOPO_CHANNEL_SEASONALITY_NEW"

    // 관계형 데이터베이스 Oracle 연결 (2)
    val dataFromOracle= spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb).
      option("user",staticUser).
      option("password",staticPw).load

    // 데이터 확인 (3)
    println(dataFromOracle.show(5))

    //데이터 정제- 지역/상품별 실적정보
    //step1. groupBy 지역, 상품
    //step2. data에서 qty만 뽑아서 정제
    //step3. 111 에 저장

    //step1
    //1) 컬럼별 인데스 생성
    var rddColumns = dataFromOracle.columns.map(x=>{
      x.toUpperCase
    })

    var regionidNo = rddColumns.indexOf("REGIONID")
    var productNo = rddColumns.indexOf("PRODUCT")
    var yearweekNo = rddColumns.indexOf("YEARWEEK")
    var qtyNo = rddColumns.indexOf("QTY")

    //2) RDD 변환
    var quiz1Rdd = dataFromOracle.rdd

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
      (x._1._1, x._1._2, qtySum) // (key, qtySum) key들을 array처럼 인식해서 오류가 남
    //x._1._1 은 key 중에서 첫번째인 x.getString(regionidNo) 의 값
    //x._1._2 는 key 중에서 두번째인 x.getString(productNo) 의 값
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

    //5) tempView 에 올림
    middleResult.createOrReplaceTempView("Haeri_MIDDLETABLE")

    val table = "KOPO_2019_HAERI"
    //append
    middleResult.write.mode("overwrite").jdbc(outputUrl, table, prop)
//    res90.write.mode("overwrite").jdbc(outputUrl, table, prop)

  }
}
