package movie_process

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by abc on 09/03/2016.
  */
object trial {


  var play_schema = StructType(Array(
    StructField("user-mboxsha",StringType, false),
    StructField("musicid",StringType, false),
    StructField("artist",StringType, true),
    StructField("plays",StringType, true)
  ))

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ConvertToParquet")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val rdd = sc.textFile("/Volumes/work/data/lastfm-dataset-360K/sample.txt")
    val splitRDD = rdd.map(_.split("\t")).map(p => Row(p(0),p(1),p(2),p(3)))

   val peopleDataFrame = sqlContext.createDataFrame(splitRDD, play_schema)

    peopleDataFrame.printSchema()

    peopleDataFrame.show()

    peopleDataFrame.write.parquet("/Volumes/work/data/lastfm-dataset-360K/trial")

    sc.stop()
  }

}

