package test

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random
import scala.util.control._
import scala.util.control.Breaks._
/**
  * Created by abc on 12/03/2016.
  */
object rdd_join_example {
// ((a,x),1)
// ((b,x),2)
  //((c,x),3)
  //((b,y),4)
  //((c,x),5)
  val in_list = List()
  val xyz = List("r","s","t","u","v","w","x","y","z")
  val freq = List(1 , 2 ,3 ,4,5,6)

  def joinRdds(sc: SparkContext):Unit = {
    val rdd = ('A' to 'Z').map(in => (in.toString,xyz(Random.nextInt(xyz.length))))
      .map{
        case (a,b) => (b,(a,freq(Random.nextInt((freq.length)))))
      }


    val rdd1 = sc.parallelize(rdd)
//    rdd1.foreach(println)


    val proxy = ('r' to 'z').map(in => (in.toString,freq(Random.nextInt((freq.length)))))

    val rdd2 = sc.parallelize(proxy)
//    rdd2.foreach(println)
    val rdd3 = sc.parallelize(List(("w",3),("v",2),("p",5),("r",2),("w",3),("x",1),("x",5),("y",5),("z",4),("z",6)))

    val result = rdd1.join(rdd2)
      .map{
        case (key,((subkey, value1),value2)) => (key,value1*value2)
      }.reduceByKey(_+_)
          .sortByKey()

    System.out.println(result.collect().mkString("\n"))

    println("****************************")

    result.mapPartitions(iter => iter.filter(_._2 >= 45)).foreach(println)


  }


  def test(sc: SparkContext): Unit ={

    val text = sc.textFile("/Volumes/work/data/reddit/test.txt")
    val out1 = text.map(line => line.split(" "))
    System.out.println(out1.collect().mkString("\n"))

    println("-------------------------------------")
    val out2 = text.flatMap(line => line.split(" "))
//    System.out.println(out2.collect().mkString("\n"))

  }


  def broadcast_rdd(sc: SparkContext): Unit ={

//
//    ((2,Y),(222,B))
//    ((3,X),(333,C))
//    ((1,Z),(111,A))
//    ((1,ZZ),(111,A))

    val rdd1 = sc.parallelize(Seq((1, "A"), (2, "B"), (3, "C")))
    val rdd2 = sc.parallelize(Seq(((1, "Z"), 111), ((1, "ZZ"), 111), ((2, "Y"), 222), ((3, "X"), 333),((4, "X"), 333)))

    val broadcastrdd1 = sc.broadcast(rdd1.collectAsMap)

    val output = rdd2.mapPartitions({ iter =>
    val m = broadcastrdd1.value
      for{
        ((a,b), value) <- iter
          if m.contains(a)
      } yield ((a,b),(value,m.get(a).get))
    },preservesPartitioning = true)

    output.foreach(println)



    var op = 10;

    breakable{
      for( line <- output.toLocalIterator){
        op -= 1
        if(op <= 0) break
        else
          println(line)
      }
    }


  }




  def mapPart(sc:SparkContext): Unit ={

    val data = Array[(String, Int)](("A1", 1), ("A2", 2),
      ("B1", 1), ("B2", 4),
      ("C1", 3), ("C2", 4)
    )
    val pairs = sc.parallelize(data, 3)

    val finalRDD = pairs.mapPartitions(iter => iter.filter(_._2 >= 2))
    // val finalRDD2 = pairs.mapPartitionsWithIndex(f, preservesPartitioning)

    finalRDD.toArray().foreach(println)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("BasicRDDJoin")
    val sc = new SparkContext(sparkConf)
    broadcast_rdd(sc)
    //test(sc)
//    mapPart(sc)
    sc.stop()
  }

}
