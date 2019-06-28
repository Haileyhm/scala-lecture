package om.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.DoubleType

object FinalExam {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
    //var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 1. data loading
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_final"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("keydata")

    println(selloutDataFromOracle.show())
    println("oracle ok")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining #1
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as String) as qty, " +
      "'test' as productname from keydata a")

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    var rawRdd = rawData.rdd

    // Global Variables //
    var VALID_YEAR = 2015
    var VALID_WEEK = 52
    var VALID_PRODUCT = Array("PRODUCT1", "PRODUCT2").toSet
    var MAX_QTY_VALUE = 9999999.0

    // groupRdd1.collectAsMap

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 3. data refining #2
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // The abnormal value is refined using the normal information
    var filterRdd = rawRdd.filter(x => {

      var checkValid = true
      // Assign yearweek information to variables
      //substring
      var year = x.getString(yearweekNo).substring(0, 4).toInt
      var week = x.getString(yearweekNo).substring(4, 6).toInt
      // Assign abnormal to variables
      // filtering
      if ((week > VALID_WEEK) ||
        (year < VALID_YEAR) ||
        (!VALID_PRODUCT.contains(x.getString(productNo)))) {
        checkValid = false
      }
      checkValid
    })
    // output: key, account, product, yearweek, qty, productname

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 4. data processing
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var mapRdd = filterRdd.map(x => {
      var qty = x.getString(qtyNo).toDouble
      if (qty > MAX_QTY_VALUE) {
        qty = MAX_QTY_VALUE
      }
      Row(x.getString(keyNo),
        x.getString(accountidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        qty, //x.getString(qtyNo),
        x.getString(productnameNo))
    })
    // output: key, account, product, yearweek, qty, productname

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 5. Exam #1 Fill in the blanks
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var groupRdd1 = mapRdd.groupBy(x => {
      (x.getString(accountidNo),
        x.getString(productNo))
    }).
      map(x => {
        // GROUP BY (accountid, product)
        var key = x._1
        var data = x._2
        // Calculate the average for each group (key, average)
        // !!!!!!!!! Blanks Start
        var size = data.size
        var volumeSum = data.map(x => {x.getDouble(qtyNo)}).sum
//        var volumeSum = groupRdd1.first._2.map(x => {x.getDouble(qtyNo)}).sum
        var avg = volumeSum / size
        (key, avg)
        // !!!!!!!!! Blanks End
        // (KEY, VALUE)
      })
    // output: (key, avg)

    // groupRdd1.collectAsMap
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 6. Exam #2 Fill in the blanks
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var groupRdd2 = mapRdd.
      groupBy(mapRddx=>{ (mapRddx.getString(accountidNo),
        mapRddx.getString(productNo))}).
      flatMap(groupByx=>{
        // GROUP BY (accountid, product)
        var key = groupByx._1
        var data = groupByx._2

        // Calculate the average for each group
        // var avg = data.

        // Calulate the ratio , ratio = each_qty / avg
        var finalData = data.map(datax=>{
          var each_qty = datax.getDouble(qtyNo)
          // !!!!!!!!! Blanks Start
          var size = data.size
          var volumeSum = data.map(datax => {datax.getDouble(qtyNo)}).sum //얘는 map 을 안 쓸 때는 안 됐어..
          var avg = volumeSum / size
          var ratio = each_qty/avg   //분모가 0일 경우에 대한 처리를 해줘야 한대
          // !!!!!!!!! Blanks End
          (datax.getString(accountidNo),
            datax.getString(productNo),
            datax.getString(yearweekNo),
            datax.getDouble(qtyNo),
            avg.toDouble,
            ratio.toDouble)})
        finalData
      })
    // output: (accountid, product,yearweek, qty, avg_qty, ratio)

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 7. Data converting (RDD -> Dataframe)
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // RDD -> Dataframe can be converted immediately without defining a row
    var middleResult = groupRdd2.toDF("REGIONID","PRODUCT","YEARWEEK","QTY","AVG_QTY","RATIO")
    println(middleResult.show)

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 8. Exam #3 Fill in the blanks
    //////////////////////////////////////////////////////////////////////////////////////////////////
    middleResult.createOrReplaceTempView("MIDDLETABLE")

    // Calculate the ratio per (region,product,week) not (region, product, year, week)
    // output: (regionid, product, week, avg_ratio)
    // !!!!!!!!! Blanks Start
    var finalResult = spark.sql("" +
      "SELECT REGIONID, PRODUCT, WEEK, AVG(RATIO) AS AVG_RATIO " +
      "FROM ( " +
      "       SELECT A.*, SUBSTR(YEARWEEK,5,6) AS WEEK" +
      "       FROM MIDDLETABLE A) " +
      "       GROUP BY REGIONID, PRODUCT, WEEK " +
      "       ORDER BY REGIONID, PRODUCT, WEEK " )

    finalResult.createOrReplaceTempView("finalResult")

    // !!!!!!!!! Blanks End

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 9. Data unloading (memory -> oracle)
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var outputUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var outputUser = "kopo"
    var outputPw = "kopo"

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    val table = "FINAL_2019ST_HAERIKIM"

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 10. Exam #4 Save your final result in oracle (FINAL_2019ST_NAME) in english
    //////////////////////////////////////////////////////////////////////////////////////////////////
    finalResult.write.mode("overwrite").jdbc(outputUrl, table, prop)
    println("finished")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // If you're bored save your data to Mysql or Oracle
    //////////////////////////////////////////////////////////////////////////////////////////////////
  }
}