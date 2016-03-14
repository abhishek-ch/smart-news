import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import logic.Utils
import org.apache.spark.mllib.linalg.Vector

/**
  * Created by abc on 25/01/2016.
  */
object ClusteringNews_Kmeans {

  val numFeatures = 1000
  val tf = new HashingTF(numFeatures)

  def featurize(s: String): Vector = {
    if(s != null){
      tf.transform(s.sliding(2).toSeq)
    }else{
      tf.transform("NA".sliding(2).toSeq)
    }


  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KMeans Clustering")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.

    import sqlContext.implicits._

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("/Users/abc/Documents/mongodump/cleandump_26Jan.csv")

    df.registerTempTable("news")
    val news_row_as_string = sqlContext.sql("SELECT data FROM news")

    val vectors = news_row_as_string.map(t=> featurize(t.getAs[String]("data"))).cache()

    import org.apache.spark.mllib.clustering.KMeans

    val model = KMeans.train(vectors,20,30)
    model.save(sc, "kmeans_cleaned_20")
    val test = sqlContext.sql("SELECT link,data from news limit 800")
    for (i <- 0 until 20) {
      println(s"\nCLUSTER $i:")
      test.foreach { t =>
        if (model.predict(featurize(t.getAs[String]("data"))) == i) {
          println(t(0) +" = "+t(1))
        }
      }
    }


//    print(vectors.collect())
  }
}
