import java.text.SimpleDateFormat
import scala.util.{Success, Try}

object Utils {

  //Function to convert to epoch time
  def epochTime(stringOfTime: String): Option[Long] = {
    val dateFormats = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if (stringOfTime.nonEmpty) {
      val parseDate = Try(dateFormats.parse(stringOfTime).getTime)
      parseDate match {
        case Success(value) => Some(value)
        case _ => None
      }
    } else None
  }

  //function to convert to the next 15 min epoch time so as to round of the start time for the reduce byKey operation.

  def epochTimeRound(stringOfTime: String) = {
    val dateFormats = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if (stringOfTime.nonEmpty){
      val longTime = dateFormats.parse(stringOfTime).getTime
      val parseDate = Try(longTime - (longTime % 900000) + 900000)
      parseDate match{
        case Success(value) => Some(value)
        case _ => None
      }
    }else None
  }

  def epochTimeToRegularFormat(longTime: Long) ={
    val dateFormats = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateRegular = Try(dateFormats.format(longTime))
    dateRegular match{
      case Success(value) => Some(value)
      case _ => None
    }
  }
}
