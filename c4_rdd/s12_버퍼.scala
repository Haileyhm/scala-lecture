package com.spark.c4_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s6_rddGroupby {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var filePath = "c:/spark/bin/data/"
    var sampleName = "kopo_product_volume.csv"

    // 파일 불러오기 함수 정의
    def csvFileLoading(workPath: String, fileName: String)
    : org.apache.spark.sql.DataFrame = {
      var outDataFrame=
        spark.read.format("csv").
          option("header","true").
          option("Delimiter",",").
          load(workPath+fileName)
      outDataFrame
    }

    var rddSampleData = csvFileLoading(filePath,sampleName)

    // 데이터 확인
    println(rddSampleData.show(5))

    // 컬럼별 인데스 생성
    var rddColumns = rddSampleData.columns.map(x=>{
      x.toUpperCase
    })

    var regionidNo = rddColumns.indexOf("REGIONID")
    var productgNo = rddColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rddColumns.indexOf("YEARWEEK")
    var volumeNo = rddColumns.indexOf("VOLUME")

//파라미터 테이블 인덱스컬럼 생성
    var paramDataColumns = rddColumns.indexOf("PARAM_REGIONID")
    var productgNo = rddColumns.indexOf("PARAM_PRODUCTGROUP")
    var yearweekNo = rddColumns.indexOf("PARAM_YEARWEEK")
    var volumeNo = rddColumns.indexOf("PARAM_VOLUME")

    // 1. RDD 변환
    var sampleRdd = rddSampleData.rdd

    // kopo_volume: regionid, productgroup, yearweek, volume
    //kopo_seasonality: REGIONID, PRODUCTGROUP, PRODUCT, YEARWEEK, YEAR, WEEK, QTY
    // 2. 지역, 상품별 평균 거래량 산출
    var groupRdd = sampleRdd.groupBy(x=>{
      (x.getString(regionidNo), x.getString(productgNo))
    }).map(x=> {
      // 그룹별 분산처리가 수행됨
      var key = x._1
      var data = x._2
      var size = data.size
      (key, size)

    })

    var groupMap = groupRdd.collectAsMap() //이렇게 해두면 나중에 groupMap("A01", "ST0001") 하면 Double = 646782.5769230769
    //dictionary 를 쓸 때는 내가 활용을 할 때 contains 를 써 줘야 해 _ groupMap.contains("A01","ST0001") 하면 함수처럼 쓸 수 있어
    var para_regionId = "A20"
    var para_productGroup = "APS"
    //groupMap.contains(para_regionId, para_productGroup) 하면, Boolean = true  로 나와

    groupMap(para_regionId, para_productGroup) //Int = 262 나와


    //flatgroup ? 이걸 하면 다 펴진대
    //group 한 걸 또 groupBy 를 해주는 거야 ...> 오잉
    var flatGroup = groupRdd.groupBy(x=> {
      (x.getString(regionidNo), //지역
        x.getString(productgNo) //상품
       )
    }).flatMap(x=>{
      //code start ?? 오잉... 디버깅을 하기 위해서 . wjsRkwl tlfgod tlzlseo....
      var key = x._1
      var data = x._2
      var size = data.size

      var outputData = data.map(x=> {
        (
          x.getString(regionidNo),
          x.getString(productgNo),
          x.getString(yearweekNo),
          x.getString(qtyNo),
          size
        )
      })
      outputData  //기존에는 여기에 (key, size) 가 들어갔는데, flatMap 쓰게 되면 모든 데이터를 그룹별로 들어왔는데 맵 연산을 한 결과를 리턴을 시키면 얘가 그냥 다 떨어지는 거야 평균을 그대로 들고서
    })




//    var groupRdd = sampleRdd.groupBy(x=>{
//      (x.getString(regionidNo),
//       x.getString(productgNo))}).
//      map(x=>{
//        // 그룹별 분산처리가 수행됨
//        var key = x._1
//        var data = x._2
//        var size = data.size
//        var volumeSum = data.map(x=>{x.getString(volumeNo).toDouble}).sum
//        var average = volumeSum/size
//        (key, average)
      })
  }
}
