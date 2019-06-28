// Functions for week Calculation
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date

// User Defined Function
// Purpose of Funtion : Calculate post week
def postWeek(inputYearWeek: String, gapWeek: Int): String = {
  var currYear = inputYearWeek.substring(0, 4).toInt
  val currWeek = inputYearWeek.substring(4, 6).toInt

  val calendar = Calendar.getInstance();
  calendar.setMinimalDaysInFirstWeek(4);
  calendar.setFirstDayOfWeek(Calendar.MONDAY);

  var dateFormat = new SimpleDateFormat("yyyyMMdd");

  calendar.setTime(dateFormat.parse(currYear + "1231"));

  var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

  var sumWeek = currWeek + gapWeek

  while(maxWeek < sumWeek) {
    currYear = currYear + 1;
    sumWeek = sumWeek - maxWeek
    calendar.setTime(dateFormat.parse(currYear + "1231"));
    maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
  }
  currYear.toString() + "%02d".format(sumWeek)
} // end of function