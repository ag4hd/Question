import Utils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.util._

import scala.language.postfixOps

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    lazy val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("class-name-test")
      .getOrCreate()



    val pathCSV = getClass().getResource("sample.csv").toString

    //errors are handle using option[] in case class
    val sampleDF = spark.read.format("csv")
                   .option("header",value = true)
                   .load(pathCSV)


    val datePattern = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"

    import spark.implicits._
    val sampleWriteDF = sampleDF
      .filter($"start".isNotNull && $"end".isNotNull) //checking for null
      .filter(col("start").rlike(datePattern) && col("end").rlike(datePattern)) //matching for the date pattern
      .map(row =>
      ImportRaw(
        start = Some(row.getAs("start")),
        end = Some(row.getAs("end"))
      )
    )

    //spark.conf.set(“spark.sql.shuffle.partitions”, 1000)
    // this is just assumned it changes as per the data size
    //calculations are inculded in the powerpoint

    val epochTimeSet = sampleWriteDF.map(row =>
      IntermediateForEpochTime(
        startTime = epochTimeRound(row.start.get), // rounding the time to 15 mins and converting it to the epoch Time
        endTime = epochTime(row.end.get) // converting it to the epoch time
      )
    )

    //creating the RDD of rounded epochtime for starttime  and epochtime for endtime
    val epochTimeRDD = epochTimeSet.rdd
      .map(row => {
        val starts = row.startTime.getOrElse().toString
        val ends = row.endTime.getOrElse().toString
        (starts,ends)
      })

    //
    val epochReduceByKey = epochTimeRDD.reduceByKey((starts, ends) => starts + "," + ends)

    val epochTimeDataFrame = epochReduceByKey.toDF

    //to count the instance of time after reduce by key and also to determine the maximum endTime
    val epochTimeDFs = epochTimeDataFrame
      .withColumn("startTime", $"_1".cast("Long"))
      .withColumn("endTime", split($"_2",",").cast("array<long>"))
      .withColumn("count", size($"endTime").cast("Long"))
      .withColumn("maxTime", array_max($"endTime"))
      .drop("_1","_2","endTime")
      .sort("startTime")
      .map(row =>
        EpochTimeDataFrame(
          startValue = row.getAs("startTime"),
          endValue = row.getAs("maxTime"),
          count = row.getAs("count")
        )
      )

    //epochTimeDFs.write.save("C:/Assignment/itvHub/src/main/resources/EpochTimeDataFrame.csv") //write to csv
    var seqSeq = Seq[(Option[String],Option[String],Long)]()

    /* for loop is used since the total rows are just 720 rows for processing the data of 1 month
    Calculation given below

     15 MIN INTERVAL so for 1 HOUR =    4 rows
                  24 HOUR = 4 * 24 =   96 rows
                 1 MONTH = 30 * 96 = 2880 rows

    */

    for(firstLoop <- epochTimeDFs.collect()){
      val endTime = firstLoop.endValue
      val countFirstLoop = firstLoop.count
      var counting : Long  = 0

        for(secondLoop <- epochTimeDFs.collect()) {
          if (secondLoop.startValue <= endTime){
            val countSecondLoop = secondLoop.count
            counting = countFirstLoop + countSecondLoop}
        }
       seqSeq =  seqSeq.union(
           Seq((
             epochTimeToRegularFormat(firstLoop.startValue),
             epochTimeToRegularFormat(firstLoop.endValue),
             counting ))
       )
    }


    val seqDF = seqSeq.toDF.map(row =>
      FinalClassConcurrentPlays(
        start = row.getAs("_1"), end = row.getAs("_2"), MaxConcurrentPlays = row.getAs("_3")
      ))

    //seqDF.write.csv("C:/Assignment/itvHub/src/main/resources/FinalClassConcurrentPlays.csv") //Dataset contains the concurrent plays value. Map is used to passed the value to case class
    // path can be mention to write output to csv

    val maxValueColumn = seqDF.agg(max("MaxConcurrentPlays")) // finding the max concurrent plays number from the column

    //using inner join to show all the max concurrent plays
    seqDF.join(maxValueColumn,
      col("MaxConcurrentPlays") === col("max(MaxConcurrentPlays)"))
      .drop("max(MaxConcurrentPlays)").show
  }
}


