///스칼라 자료형
//1.숫자형

// Practice : 다음 수(a,b,c)를 소수점 2자리로 반올림하세요
//(*심화: if문을 응용해서 3개의 수가 100이 되게 하세요)

var a = 15.125222;
var b = 15.147218;
var c = 69.72756;

if (a+b+c) !=100
{
  a = Math.round(a);
  b = Math.round(b);
  c = Math.round(c);

  var total = a + b + c;

  print(total);
}

//2. 문자형
//Practice : “SEC 20180212 250” 선언 후
// stock_name, date, value 변수에 각각 담으세요.
//필요 문자열 추출
var pracStr = "SEC 20190828 250";
var stock_name = pracStr.substring(0,3);
var date = pracStr.substring(4,12);
var value = pracStr.substring(13,16);


//Practice : “SEC,201702,3000,10,Y” 를 콤마로 구분하여 각각 변수에 담기
// (Split 함수활용하기)

var pracStr1 = "SEC,2019,3000,10,Y"

var pracStr11 = pracStr1.split(",") //배열이라 인덱스 될 줄 알았는데 안됨..

var one = pracStr1.substring(0,3)
var two = pracStr1.substring(4,8)
var three = pracStr1.substring(9,13)
var four = pracStr1.substring(14,16)
var five = pracStr1.substring(17,18)

//Practice : 문자열 변수 “2015W15”에서 1주를 뺀 값 201514값을 구하세요
//(단, w값은 다른값으로 변할 수 있는점을 유의)

var week1 = "2015W15";

var week2 = week1.replace("W","").toDouble;
var resultweek = week2 - 1;


//3.리스트 _ List.empty 로 빈 리스트 생성하면 type: nothing 이므로 주의
//역순 정렬
//priceList = priceList.sortBy(x=>{-x})
//priceList = priceList.sortWith(_>_)

//Practice : 8000, 9000, 10000, 2000 숫자 리스트를 생성 후 평균을 구하세요

var pracList = List(8000,9000, 10000, 2000);

//var pracListmin = List.mean() -> 이건 안되네~~
//그럼 합을 사이즈로 나누자

var sumList = pracList.sum
var sizeList = pracList.size

var pracMean = sumList / sizeList


//4.배열(Array)
//Practice : 100,200,300,400,500,600,700 배열 생성 후
// sliding 함수를 활용하여 (100,200,300), (200,300,400), (300,400,500),로 분리해 보세요


var pracArray = Array(100,200,300,400,500,600,700)

pracArray.sliding(3).toList;   //toList 안 쓰면 non-empty iterator 오류발생



//5.데이터프레임
//Practice :
//a,b,c
//1,2,3
//4,5,6
//txt 생성 후 dataframe으로 변환하세요 List명.toDF *Array는 안됨

//var pracDfList = List(("a","b","c"),(1,2,3),(4,5,6))

val conf = new SparkConf().setAppName(appName).setMaster("local")
val sc = SparkContext(conf)

// 될 줄 알았는데 모르겠다ㅎㅎ   -> txt 파일도 format("csv") 네
val HOME = "c:/spark/bin/"

var pracDfData =
  spark.read.format("csv").// 괜히 txt 바꿨다 헤맴
    option("header","true").
    option("Delimiter",",").
    load("c:/spark/bin/data/"+"pracDf.txt")


//6.rdd
//데이터프레임명.rdd.collect.foreach(println)




















