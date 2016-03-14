/**
  * Created by abc on 24/01/2016.
  */
import scala.collection.mutable.ArrayBuffer
import java.util.Scanner
import java.io.{FileWriter, File}
import scala.collection.JavaConversions._



object mergecsvtoSingleString extends App {

  // each row is an array of strings (the columns in the csv file)
  val rows = ArrayBuffer[Array[String]]()
  var fullText: String = ""

  val scanner = new Scanner(new File("/Volumes/work/project/github/machinelearning-news/commonwords.txt")).useDelimiter(",")
  val stopwords = scanner.map(_.trim).toList
  val fw = new FileWriter("/Users/abc/Documents/mongodump/test.txt", true)

  try {
  using(scala.io.Source.fromFile("/Users/abc/Documents/mongodump/newswebdump_1.csv")) { source =>
    for (line <- source.getLines) {
      val words = line.split(",").map(_.trim)
      if (words.length > 1) {
        fw.write(words(1))
        println(words(1))
      }
    }
  }
}finally fw.close()




  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }

}
