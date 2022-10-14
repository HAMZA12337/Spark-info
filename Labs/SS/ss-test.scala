import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

val ssc = new StreamingContext(sc, Seconds(1))

val lines = ssc.socketTextStream("localhost", 7777)

val pass = lines.map(_.split(",")).
    map(pass=>(pass(15), pass(7).toInt)).
    reduceByKey(_+_)

pass.print()

ssc.start()
ssc.awaitTermination()