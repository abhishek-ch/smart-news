package first_pipeline

import scala.collection.mutable.ArrayBuffer

/**
  * Created by abc on 15/02/2016.
  */
class Handler extends java.io.Serializable{

  var currkey: String = ""
  var wordBuffer = ArrayBuffer[String]()
}
