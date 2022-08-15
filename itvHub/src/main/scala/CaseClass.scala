case class ImportRaw(
                    start : Option[String],
                    end : Option[String]
                    )

case class IntermediateForEpochTime(
                        startTime : Option[Long],
                        endTime : Option[Long]
                      )

case class EpochTimeDataFrame(
                           startValue : Long,
                           endValue : Long,
                           count : Long
                         )

case class FinalClassConcurrentPlays(
                      start : String,
                      end : String,
                      MaxConcurrentPlays : Long
                    )







