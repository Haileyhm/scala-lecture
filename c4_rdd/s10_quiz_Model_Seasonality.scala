package com.haiteam



object Seasonality {

  def main(args: Array[String]): Unit = {

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Library Definition ////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    import org.apache.spark.sql.SparkSession
    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Function Definition ////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // Function: Return movingAverage result
    // Input:
    //   1. Array : targetData: inputsource
    //   2. Int   : myorder: section
    // output:
    //   1. Array : result of moving average
    def movingAverage(targetData: Array[Double], myorder: Int): Array[Double] = {
      val length = targetData.size
      if (myorder > length || myorder <= 2) {
        throw new IllegalArgumentException
      } else {
        var maResult = targetData.sliding(myorder).map(_.sum).map(_ / myorder)

        if (myorder % 2 == 0) {
          maResult = maResult.sliding(2).map(_.sum).map(_ / 2)
        }
        maResult.toArray
      }
    }

    ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
    var spark = SparkSession.builder().config("spark.master","local").getOrCreate()

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Loading   ////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // Path setting
//    var dataPath = "./data/"
//    var mainFile = "kopo_channel_seasonality_ex.csv"
//    var subFile = "kopo_product_master.csv"
//
//    var path = "c:/spark/bin/data/"
//
//    // Absolute Path
//    //kopo_channel_seasonality_input
//    var mainData = spark.read.format("csv").option("header", "true").load(path + mainFile)
//    var subData = spark.read.format("csv").option("header", "true").load(path + subFile)
//
//    spark.catalog.dropTempView("maindata")
//    spark.catalog.dropTempView("subdata")
//    mainData.createTempView("maindata")
//    subData.createOrReplaceTempView("subdata")
//
//    /////////////////////////////////////////////////////////////////////////////////////////////////////
//    ////////////////////////////////////  Data Refining using sql////////////////////////////////////////
//    /////////////////////////////////////////////////////////////////////////////////////////////////////
//    var joinData = spark.sql("select a.regionid as accountid," +
//      "a.product as product, a.yearweek, a.qty, b.productname " +
//      "from maindata a left outer join subdata b " +
//      "on a.productgroup = b.productid")
//
//    joinData.createOrReplaceTempView("keydata")
    // 1. data loading
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var staticUrl = "jdbc:oracle:thin:@192.168.0.10:1521/XE"
    staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("keydata")

    println(selloutDataFromOracle.show())
    println("oracle ok")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining
    //////////////////////////////////////////////////////////////////////////////////////////////////

    //    var mainDataSelectSql = "select regionid, regionname, ap1id, ap1name, accountid, accountname," +
    //      "salesid, salesname, productgroup, product, item," +
    //      "yearweek, year, week, " +
    //      "cast(qty as double) as qty," +
    //      "cast(target as double) as target," +
    //      "idx from selloutTable where 1=1"
    var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as String) as qty, " +
      "'test' as productname from keydata a" )

    rawData.show(2)

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    var rawRdd = rawData.rdd

//map 써서 (키, (사이즈, 평균, 표준편차)) 구하는 중
    var mapMapRdd = mapRdd.groupBy(x=>{
      (x.getString(accountidNo), x.getString(productNo))
    }).map(x=> {
      // 그룹별 분산처리가 수행됨
      var key = x._1
      var data = x._2
      var size = data.size
      var qtySum = data.map(x=>{x.getString(qtyNo).toDouble}).sum
      var map_means = qtySum/size
      var map_stdev = data.stddev(qty)
      (key, size, map_means)
    })


    var flatGroupAnswer = mapRdd.groupBy(x=>{
      (x.getString(keyNo) //지역, 상품
        )
    }).flatMap(x=>{
      //코드 시작
      var key = x._1
      var data = x._2
      //size, average, stddev
      //step1: Calculate size
      var size = data.size

      //step2: Average
      var qtyList = data.map(x=>{x.getDouble(qtyNo)})
      var qtyArray = qtyList.toArray
      var qtySum = qtyArray.sum //array 에서는 sum 함수를 쓸 수 있음

      var qtyMean = if(size!=0){
        qtySum / size
      }else{
        0.0
      }
      //step3: 표준편차
      //각 데이터 - 평균
      var stdev = if(size!=0){
        Math.sqrt(
        qtyList.map(x => { Math.pow(x-qtyMean,2)}).sum / size)
      }else{
        0.0
      }

      var stdev2 = qtyStddev = stdStats.stddev(qtyArray)

      //output
      var outputData = data.map(x=>{
        var org_qty = x.getDouble(qtyNo)
        var seasonality = if(qytMean!=0){
          org_qty/qtyMean
        }else{
         0.0
        }
        (
        x.getString(keyNo),
        x.getString(accountidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        size,
        qtyMean,
        stdev
      })
      outputData
      //코드 끝
    })




    var x = flatGroupAnswer.first





    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // The abnormal value is refined using the normal information
    var filterRdd = rawRdd.filter(x=>{

    // Data comes in line by line
    var checkValid = true
    // Assign yearweek information to variables
    var week = x.getString(yearweekNo).substring(4,6).toInt
    // Assign abnormal to variables
    var standardWeek = 52

    // filtering
    if (week > standardWeek)
    {
      checkValid = false
    }
    checkValid
    })

    // key, account, product, yearweek, qty, productname
    var mapRdd = filterRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var maxValue = 700000
      if(qty > 700000){qty = 700000}
      Row( x.getString(keyNo),
        x.getString(accountidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        qty, //x.getString(qtyNo),
        x.getString(productnameNo))
    })

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Processing        ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // Distributed processing for each analytics key value (regionid, product)
    // 1. Sort Data
    // 2. Calculate moving average
    // 3. Generate values for week (out of moving average)
    // 4. Merge all data-set for moving average
    // 5. Generate final-result
    var groupRddMapExp = mapRdd.
      groupBy(x=>{ (x.getString(keyNo), x.getString(accountidNo)) }).
      flatMap(x=>{

        var key = x._1
        var data = x._2

        // 1. Sort Data
        var sortedData = data.toSeq.sortBy(x=>{x.getString(yearweekNo).toInt})
        var sortedDataIndex = sortedData.zipWithIndex

        var sortedVolume = sortedData.map(x=>{ (x.getDouble(qtyNo))}).toArray
        var sortedVolumeIndex = sortedVolume.zipWithIndex

        var scope = 17
        var subScope = (scope.toDouble / 2.0).floor.toInt

        // 2. Calculate moving average
        var movingResult = movingAverage(sortedVolume,scope)

        // 3. Generate value for weeks (out of moving average)
        var preMAArray = new ArrayBuffer[Double]
        var postMAArray = new ArrayBuffer[Double]

        var lastIndex = sortedVolumeIndex.size-1
        for (index <- 0 until subScope) {
          var scopedDataFirst = sortedVolumeIndex.
            filter(x => x._2 >= 0 && x._2 <= (index + subScope)).
            map(x => {x._1})

          var scopedSum = scopedDataFirst.sum
          var scopedSize = scopedDataFirst.size
          var scopedAverage = scopedSum/scopedSize
          preMAArray.append(scopedAverage)

          var scopedDataLast = sortedVolumeIndex.
            filter(x => { (  (x._2 >= (lastIndex - subScope - index)) &&
                             (x._2 <= lastIndex)    ) }).
            map(x => {x._1})
          var secondScopedSum = scopedDataLast.sum
          var secondScopedSize = scopedDataLast.size
          var secondScopedAverage = secondScopedSum/secondScopedSize
          postMAArray.append(secondScopedAverage)
        }

        // 4. Merge all data-set for moving average
        var maResult = (preMAArray++movingResult++postMAArray.reverse).zipWithIndex

        // 5. Generate final-result
        var finalResult = sortedDataIndex.
        zip(maResult).
        map(x=>{

          var key = x._1._1.getString(keyNo)
          var regionid = key.split("_")(0)
          var product = key.split("_")(1)
          var yearweek = x._1._1.getString(yearweekNo)
          var volume = x._1._1.getDouble(qtyNo)
          var movingValue = x._2._1
          var ratio = 1.0d
          if(movingValue != 0.0d){
            ratio = volume / movingValue
          }
          var week = yearweek.substring(4,6)
          Row(regionid, product, yearweek, week, volume.toString, movingValue.toString, ratio.toString)
        })
        finalResult
    })

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data coonversion (RDD -> Dataframe) ///////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    val middleResult = spark.createDataFrame(groupRddMapExp,
      StructType(Seq(StructField("REGIONID", StringType),
        StructField("PRODUCT", StringType),
        StructField("YEARWEEK", StringType),
        StructField("WEEK", StringType),
        StructField("VOLUME", StringType),
        StructField("MA", StringType),
        StructField("RATIO", StringType))))

    middleResult.createOrReplaceTempView("middleTable")

    var finalResultDf = spark.sqlContext.sql("select REGIONID, PRODUCT, WEEK, AVG(VOLUME) AS AVG_VOLUME, AVG(MA) AS AVG_MA," +
      " AVG(RATIO) AS AVG_RATIO from middleTable group by REGIONID, PRODUCT, WEEK")

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Unloading        ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // File type
    //    finalResultDf.
    //      coalesce(1). // ���ϰ���
    //      write.format("csv").  // ��������
    //      mode("overwrite"). // ������ append/overwrite
    //      save("season_result") // �������ϸ�
    //    println("spark test completed")
    // Database type
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "kopo_new_logic_result"

    staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"

    finalResultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)
    println("spark test completed")
  }
}
