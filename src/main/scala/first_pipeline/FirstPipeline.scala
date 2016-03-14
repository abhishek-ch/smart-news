package first_pipeline

import java.io.{PrintWriter, File}
import java.util.Scanner

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.io.Source
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import scala.util.parsing.json.JSONObject

/**
  * Created by abc on 15/02/2016.
  */
object FirstPipeline {

  val scanner = new Scanner(new File("/Volumes/work/project/github/machinelearning-news/commonwords.txt")).useDelimiter(",")
  val stopwords = scanner.map(_.trim).toList
  val handler = new Handler()

  val s1 = Source.fromFile("/Volumes/work/project/github/machinelearning-news/positive-words.txt").mkString
  val pos_words = s1.split("\\s+")
  val s2 = Source.fromFile("/Volumes/work/project/github/machinelearning-news/negative-words.txt").mkString
  val neg_words = s1.split("\\s+")


  def startProcessing(key: DStream[String], value: DStream[String]): Unit = {
    key.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        handler.currkey = rdd.first()
      }
    })


    processFurther(value)
  }


  def extractImportantFeatures(word: String): (String, Int) = {
    if (!stopwords.contains(word.toLowerCase()) && word.length > 2) {
      handler.wordBuffer += word
      return (word, 1)
    }

    return ("stop_words", 1)
  }


  def calculateSentiment(line: String): Map[String, Any] = {
    val words: Array[String] = line.split(" ")
    var numOfPosWords: Int = 0
    var numOfNegWords: Int = 0
    var numofWords: Int = 0
    var nontraceblewords = Set("")
    for (word <- words) {
      val each: String = word.toLowerCase()
      if (!stopwords.contains(each) && pos_words.contains(each)) {
        numOfPosWords += 1
      } else if (!stopwords.contains(each) && neg_words.contains(each)) {
        numOfNegWords += 1
      } else if (!stopwords.contains(each) && (each.length > 2) && (!isNumeric(each))) {
        nontraceblewords += each
      }
      numofWords += 1
    }
    return Map[String, Any](
      "trend" -> handler.currkey,
      "positive_words" -> numOfPosWords,
      "negative-words" -> numOfNegWords,
      "total_words" -> numofWords,
      "unknown_words" -> nontraceblewords
    )
    //new Tuple5[String,Int,Int,Int,String](handler.currkey,numOfPosWords,numOfNegWords,numofWords,nontraceblewords)
  }


  def isNumeric(input: String): Boolean = input.forall(_.isDigit)

  //  val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  val data_save_path: String = "/Volumes/work/data/others/mlnews1/"

  def processFurther(value: DStream[String]): Unit = {
    value.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val splitter: RDD[Array[String]] = rdd.map(line => line.split(" "))

        val words: RDD[String] = rdd.flatMap(line => line.split(" "))

        //sort by value leads to following pipeline
        //http://stackoverflow.com/questions/24656696/spark-get-collection-sorted-by-value

        val counts = words.map(extractImportantFeatures).
          reduceByKey { case (x, y) => x + y }.map(item => item.swap)
          .sortByKey(false, 1)
          .map(item => item.swap)
        counts.foreach(println)

        counts.saveAsTextFile("file://" + data_save_path + handler.currkey + "_frequency")

        val sentiment = calculateSentiment(rdd.first())
        val nObject: JSONObject = scala.util.parsing.json.JSONObject(sentiment)
        println("SETETETETETTETETETET====>>>>  " + sentiment)
        Files.write(Paths.get(data_save_path + handler.currkey + "_sentiment"), nObject.toString().getBytes(StandardCharsets.UTF_8))
      }
    })
  }


  def main(args: Array[String]) {

    val Array(brokers, topics) = Array("localhost:9092", "tweetclean")


    // Create context with 1 second batch interval

    val sparkConf = new SparkConf().setAppName("FirstPipeline")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val cleanedtweets = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    startProcessing(cleanedtweets.map(_._1), cleanedtweets.map(_._2))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
