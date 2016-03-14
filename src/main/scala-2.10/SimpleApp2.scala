import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by abc on 23/01/2016.
  */
object SimpleApp2 {
  def main (args: Array[String]) {
    val logFile = "file:///Volumes/work/data/db/test/dataset.json" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
