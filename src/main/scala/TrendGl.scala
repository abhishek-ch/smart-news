package logic

import scala.collection.immutable.HashMap

/**
  * Created by abc on 12/02/2016.
  */
class TrendGl extends java.io.Serializable{
//  you are supposed to make the class serializable
  //reason http://stackoverflow.com/questions/22592811/task-not-serializable-java-io-notserializableexception-when-calling-function-ou

  var trend: String = ""
  var prevtrend:String = ""
  var trendTweetMap:Map[String,String] = Map()

  def update(tr: String): String = {
    if(tr != trend){
      prevtrend = trend
//      println("PREVIOSUss TREND   =>> "+prevtrend+" Size "+trendTweetMap.keys.size)
      trend = tr
      return prevtrend
    }
    return null
  }


  def update_map(value: String) ={
    val existingValue: String = trendTweetMap.getOrElse(trend,"").trim()
    trendTweetMap += (trend -> (existingValue+" "+value.trim))
  }

  def isStatementAlreadyExist(toMatch: String): Boolean = {
    return trendTweetMap.get(trend).toString.contains(toMatch)
  }

}
