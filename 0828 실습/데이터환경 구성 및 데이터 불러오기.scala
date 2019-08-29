//Review- 자료형
//Practice : 배열 (22,33,50,70,90,100) 을 생성한 뒤
//filter 함수를 활용하여 3자리수인 데이터만 남기세요

var reviewArray = Array(22,33,50,70,90,100);

var answer = reviewArray.filter(x=>{
  var data = x.toString
  var dataSize = data.size

  dataSize > 2
})


//Practice : 배열 (22,33,50,70,90,100) 을 생성한 뒤
//filter 함수를 활용하여 끝 자리수가 0인 데이터만 남기세요

var reviewArray2 = Array(22,33,50,70,90,100);

var answer2 = reviewArray2.filter(x=>{
  x%10 == 0
})


