import java.io.File
import java.util.Scanner

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import scala.collection.JavaConversions._

/**
  * Created by abc on 24/01/2016.
  */
object Word2VecNewsSynonym {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Find Synonym")
    val sc = new SparkContext(conf)

    val scanner = new Scanner(new File("/Volumes/work/project/github/machinelearning-news/commonwords.txt")).useDelimiter(",")
    val stopwords = scanner.map(_.trim).toList


    val input = sc.textFile("/Users/abc/Documents/mongodump/test.txt").map(line => line.split(" ").filter((word:String) => !stopwords.contains(word.toLowerCase)).toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("Modi", 40)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = Word2VecModel.load(sc, "myModelPath")

  }
}
