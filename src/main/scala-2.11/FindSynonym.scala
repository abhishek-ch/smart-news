import java.io.File
import java.util.Scanner
import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
//import org.bson.{BasicBSONObject, BSONObject}
//import scala.io.Source
//import org.bson.BSONObject
//import org.bson.BasicBSONObject
//import com.mongodb.hadoop.{
//MongoInputFormat, MongoOutputFormat,
//BSONFileInputFormat, BSONFileOutputFormat}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

/**
  * Created by abc on 23/01/2016.
  */
object FindSynonym {
//http://bigdatasciencebootcamp.com/posts/Part_3/clustering_news.html
  def main(args: Array[String]) {

//    val conf = new SparkConf().setAppName("Find Synonym")
//    val sc = new SparkContext(conf)
//
//    val config = new Configuration()
//    config.set("mongo.input.uri",
//      "mongodb://localhost:27017/news.example")
//    config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/news.output")
//
//    // Create a separate Configuration for saving data back to MongoDB.
//    val outputConfig = new Configuration()
//    outputConfig.set("mongo.output.uri",
//      "mongodb://localhost:27017/output.collection")
//
//
//    val scanner = new Scanner(new File("commonwords.txt")).useDelimiter(",")
//    val stopwords = scanner.map(_.trim).toList
//
//    println("lines "+stopwords(2))
//
//    // Create an RDD backed by the MongoDB collection.
//    val mongoRDD = sc.newAPIHadoopRDD(
//      config,                // Configuration
//      classOf[MongoInputFormat],  // InputFormat
//      classOf[Object],            // Key type
//      classOf[BSONObject])        // Value type
//
//
//    val allDataRDD = mongoRDD.flatMap(arg => {
//      var str = arg._2.get("item").toString
//      str = str.toLowerCase().replaceAll("[.,!?\n]", " ")
//      str.split(" ").filter((i:String) => !(stopwords.contains(i)))
//    })
//
//  val red =  allDataRDD.reduce((a,b) => (a+" "+b))
//
//
//
//  red.map(arg => {
//    var bson = new BasicBSONObject()
//    bson.put("word", arg.toString)
//  })
//
//    val saveRDD = allDataRDD.map(arg => {
//      var bson = new BasicBSONObject()
//      bson.put("word", arg)
//      bson.put("count", "1")
//      (null, bson)
//    })
//
//    // Only MongoOutputFormat and config are relevant
//    saveRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)

  }
}
