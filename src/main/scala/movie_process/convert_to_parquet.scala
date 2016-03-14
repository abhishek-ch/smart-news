package movie_process

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext


/**
  * Created by abc on 08/03/2016.
  * https://developer.ibm.com/hadoop/blog/2015/12/03/parquet-for-spark-sql/
  */
object convert_to_parquet {


  def convert_to_parquet(sqlContext: SQLContext, filePath: String, schema: StructType, tableName: String): Unit ={
    val df = sqlContext.read.format("com.databricks.spark.csv").
      schema(schema).option("delimiter","\t").option("escape","\"").load(filePath)

    df.write.parquet("/Volumes/work/data/lastfm-dataset-360K/"+tableName)
  }


  def read_file_as_text_convert_df_save_as_parquet(sc: SparkContext,sqlContext: SQLContext,filePath: String,tableName: String): Unit ={
    val rdd = sc.textFile(filePath)
    val splitRDD = rdd.map(_.split("\t")).map(p => Row(p(0),p(1),p(2),p(3)))

    //convert to dataframe
    val df = sqlContext.createDataFrame(splitRDD, play_schema)
    df.show()

    df.write.parquet("/Volumes/work/data/lastfm-dataset-360K/"+tableName)
    df.show()
  }

  var profile_schema = StructType(Array(
    StructField("user-mboxsha", StringType, false),
    StructField("gender", StringType, true),
    StructField("age", IntegerType, true),
    StructField("country", StringType, true),
    StructField("signup", StringType, true)))

  var play_schema = StructType(Array(
    StructField("user-mboxsha",StringType, false),
    StructField("musicid",StringType, false),
    StructField("artist",StringType, true),
    StructField("plays",StringType, true)
  ))

  val reddit_schema = StructType(Array(
    StructField("archived", BooleanType, true),
    StructField("author", StringType, true),
    StructField("author_flair_css_class", StringType, true),
    StructField("body", StringType, true),
    StructField("controversiality", LongType, true),
    StructField("created_utc", StringType, true),
    StructField("distinguished", StringType, true),
    StructField("downs", LongType, true),
    StructField("edited", StringType, true),
    StructField("gilded", LongType, true),
    StructField("id", StringType, true),
    StructField("link_id", StringType, true),
    StructField("name", StringType, true),
    StructField("parent_id", StringType, true),
    StructField("retrieved_on", LongType, true),
    StructField("score", LongType, true),
    StructField("score_hidden", BooleanType, true),
    StructField("subreddit", StringType, true),
    StructField("subreddit_id", StringType, true),
    StructField("ups", LongType, true)))

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ConvertToParquet")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/sql/hive/HiveFromSpark.scala

    // A hive context adds support for finding tables in the MetaStore and writing queries
    // using HiveQL. Users who do not have an existing Hive deployment can still create a
    // HiveContext. When not configured by the hive-site.xml, the context automatically
    // creates metastore_db and warehouse in the current directory.
//    val hiveContext = new HiveContext(sc)
//
//    import hiveContext.implicits._
//    import hiveContext.sql
//    convert_to_parquet(sqlContext,"/Volumes/work/data/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv",profile_schema,"profile_pqt")
    read_file_as_text_convert_df_save_as_parquet(sc,sqlContext,"/Volumes/work/data/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv","play_pqt")


    sc.stop()


  }
}
