import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

// import 문 생략
object s1_dataLoadingFile {

  //데이터 불러오기
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
    var paramFile = "pro_actual_sales.csv"

    // 절대경로 입력
    var paramData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+paramFile)

    // 데이터 확인 (3)
    print(paramData.show)
  }


  //컬럼 인덱싱
  var rddColumns = paramData.columns

  var regionSeg1No = rddColumns.indexOf("regionSeg1")
  var productSeg2No = rddColumns.indexOf("productSeg2")
  var yearweekNo = rddColumns.indexOf("yearweek")
  var qtyNo = rddColumns.indexOf("qty")

  // 1. RDD 변환
  var quiz2Rdd = paramData.rdd

  // regionid, productgroup, yearweek, volume
  // 2. regionseg1, productseg2, yearweek별 평균 거래량 산출

//1차 시도
//  var mappedRdd = filteredRdd.map(row=>{
//    // 거래량 컬럼정보 변수 맵핑
//    var volume = row.getString(volumeNo).toDouble
//
//    // 한계치 이상 시 한계치 값으로 변경
//    if(volume >= MAX_VOLUME){ volume = MAX_VOLUME}
//
//    // 출력 컬럼정보 생성
//    Row(row.getString(regionidNo)
//      , row.getString(productgNo)
//      , row.getString(yearweekNo)
//      , volume
//      , 0) //=======================================================================> 컬럼이 만들어지면서 생겨
//  })


//2차 시도
//def mapFunc(key: String): Int = {
//  if(key == "regionseg1"){
//    key = "regionSeg1No"}
//  else if(key == "productseg2"){
//    key = "productSeg2No"
//  }else if(key == "") {
//    key = "yearweekNo"
//  }
//  quiz2Rdd.groupBy(x=>{
//    (x.getString(key),
//      x.getString(qtyNo))}).
//    Map(x=>{
//      // 그룹별 분산처리가 수행됨
//      var key = x._1
//      var data = x._2
//      var finalData = data.map(x=>{
//        (x.getString(key),
//          x.getString(qtyNo),
//          average)
//      })
//      finalData
//    })
//}

  var regionSeg1No = rddColumns.indexOf("regionSeg1")
  var productSeg2No = rddColumns.indexOf("productSeg2")
  var yearweekNo = rddColumns.indexOf("yearweek")
  var qtyNo = rddColumns.indexOf("qty")

//3차 시도
  var quiz2groupRdd = quiz2Rdd.groupBy(x=>{
    (x.getString(regionSeg1No), x.getString(productSeg2No), x.getString(yearweekNo))
  }).map(x=> {
    // 그룹별 분산처리가 수행됨
    var key = x._1
    var data = x._2
    //data 는 그룹한 걸로 묶은 나머지 애들을 모두 포함.
    // 그 중에서 qtyNo 의 String 만 뽑아서 sum 을 해줘
    var qtySum = data.map(x=>{x.getString(qtyNo).toDouble}).sum
    (key, qtySum)
  })

  var quiz2groupMap = quiz2groupRdd.collectAsMap() //이렇게 해두면 나중에 groupMap("A01", "ST0001") 하면 Double = 646782.5769230769
  //dictionary 를 쓸 때는 내가 활용을 할 때 contains 를 써 줘야 해 _ groupMap.contains("A01","ST0001") 하면 함수처럼 쓸 수 있어
//  var para_regionId = "A20"
//  var para_productGroup = "APS"
//  //groupMap.contains(para_regionId, para_productGroup) 하면, Boolean = true  로 나와
//
//  groupMap(para_regionId, para_productGroup)


//만든 맵 함수
//답:
quiz2groupMap("A01","PG03", "201619")



