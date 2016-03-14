package kafka

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable
import java.util.concurrent.Future

/**
  * Created by abc on 14/02/2016.
  */
class CustomKafkaProducer(brokers: String, topic: String) extends Serializable{

  //constructor
  val props = new util.HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")

  @transient val producer = new KafkaProducer[String, String](props)
  @transient private lazy val pending = mutable.Stack[Future[_]]()

  println(" ************** Kafka Producer Created *******************")

  def write(key: String, content: String): Unit ={
    val record = new ProducerRecord[String, String](topic, key,content)
    pending push producer.send(record)
  }

  def close(): Unit = producer.close()


}
