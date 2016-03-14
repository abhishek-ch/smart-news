package kafka

import _root_.kafka.serializer.{StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import logic.TrendGl



/**
  * Created by abc on 31/01/2016.
  */


object KafkaWordCount {



  /**
    *
    * @param message
    * @param number expected number of  tweets variables
    * @return
    */
  var trend: String = ""
  var foundNewKey: String = ""
  def cleanTweets(message: DStream[String], number : Int, key: DStream[String]): Unit = {


    key.foreachRDD(rddi => {
      if(!rddi.isEmpty()){
        foundNewKey = global.update(rddi.first())
        if(foundNewKey != null){
          //update kafka with new value
          val maybeString: Option[String] = global.trendTweetMap.get(foundNewKey)
          if(maybeString != None){
            println("PRINTING ******* "+maybeString.get)
            kafka_Producer.write(global.prevtrend,maybeString.get)
          }

        }
      }})

    cleansing(message,number)

  }


  def cleansing(message: DStream[String], number : Int): Unit ={

    message.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        val rdds: RDD[Array[String]] = rdd.map(line => line.split(";"))
        for(each <- rdds){
          if(each.length > 2){
            var input: String = each(1)
            input = input.replaceAll(global.trend,"")
            input = input.replaceAll("RT ","")
            if(!global.isStatementAlreadyExist(input)){
              var expr = """http/|https[^\\s]+""".r
              input =  expr.replaceAllIn(input ,"")
              expr = """#(\w+)""".r
              input =  expr.replaceAllIn(input ,"")
              expr = """@(\w+)""".r
              input =  expr.replaceAllIn(input ,"")
              expr = """[^A-Za-z0-9 ]""".r
              input =  expr.replaceAllIn(input ," ")
              global.update_map(input)
            }else{
              println("----------------------- FOUND ************** "+input)
            }

          }
        }}})
  }

  val global = new TrendGl()
  val kafka_Producer = new CustomKafkaProducer("localhost:9092","tweetclean")

  def main(args: Array[String]) {

    val Array(brokers, topics) = Array("localhost:9092","tweet")


    // Create context with 1 second batch interval

    val sparkConf = new SparkConf().setAppName("BasicTweetProcessing")
    val ssc = new StreamingContext(sparkConf, Seconds(1))


    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    messages.cache()
    cleanTweets(messages.map(_._2),4,messages.map(_._1))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

    kafka_Producer.close()
  }


}
