package com.devin.sparkk

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkContext._
import org.apache.spark.storage._
import org.apache.spark._
import org.apache.spark.streaming._


import org.apache.spark.streaming.dstream._

import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer, ConsumerRecord }

import java.util.HashMap
import java.nio.ByteBuffer
import scala.util.Random
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import java.util

import scala.collection.JavaConverters._
import java.util.Properties
import java.net.Socket
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

object newap {
  val stopActiveContext = true
  val batchIntervalSeconds = 10
  val eventsPerSecond = 1000
  val host = "localhost"
  val port = 8080

  class DummySource(ratePerSec: Int, host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
    println("inside class DUMMYYYY  ... ")
    def onStart() {
      println("debug1 **********")
      // Start the thread that receives data over a connection
      new Thread("Dummy Source") {
        override def run() { receive() }
      }.start()
    }

    def onStop() {
      // There is nothing much to do as the thread calling receive()
      // is designed to stop by itself isStopped() returns false
    }

    //** Create a socket connection and receive data until receiver is stopped *//*
    private def receive() {
      val socket = new Socket(host, port)
      while (!isStopped()) {
        store("I am a dummy source " + Random.nextInt(10))
        Thread.sleep((1000.toDouble / ratePerSec).toInt)
      }
    }
    //      onStart()

  }
  def main(args: Array[String]): Unit = {

    import scala.util.Random
    import org.apache.spark.streaming.receiver._

    println("inside main....!!!")
    var newContextCreated = false // Flag to detect whether new context was created or not
    val kafkaBrokers = "localhost:9092" // comma separated list of broker:host

    // Function to create a new StreamingContext and set it up
    def creatingFunc(): StreamingContext = {
      val conf = new SparkConf()
        .setAppName("stok")
        .setMaster("local[*]")
      val sc = new SparkContext(conf)

      // "true"  = stop if any existing StreamingContext is running;              
      // "false" = dont stop, and let it run undisturbed, but your latest code may not be used

      // === Configurations for Spark Streaming ===
      // For the dummy source

      // Verify that the attached Spark cluster is 1.4.0+
      //require(sc.version.replace(".", "").toInt >= 140, "Spark 1.4.0+ is required to run this notebook. Please attach it to a Spark 1.4.0+ cluster.")

      // Create a StreamingContext
      val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))

      // Create a stream that generates 1000 lines per second
      val stream = ssc.receiverStream(new DummySource(eventsPerSecond, host, port))

      val wordStream = stream.flatMap { _.split(" ") }
      val wordCountStream = wordStream.map(word => (word, 1)).reduceByKey(_ + _)

      wordCountStream.foreachRDD(rdd => {
        System.out.println("# events = " + rdd.count())

        rdd.foreachPartition(partition => {
          println("inside !!! ### forearch partition !!! ###")
          // Print statements in this section are shown in the executor's stdout logs
          val kafkaOpTopic = "test-output"
          val props = new HashMap[String, Object]()
          //          val props = new Properties()
          //          props.put("bootstrap.servers", "localhost:9092")
          //          props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
          //          props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
          //          props.put("group.id", "something")
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
          println("PROPS SET...")
          val producer = new KafkaProducer[String,String](props)
          
          println("producer rocks ******* ->" +producer)
          partition.foreach(record => {
            println(record)
            val data = record.toString
            println("Record ---------->" +record)
            val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
             println("Messsage to be send ---------->" +message)
            producer.send(message)
          })
          producer.close()

          //          val propc = new HashMap[String, Object]()
          //          val propc = new Properties()
          //          props.put("bootstrap.servers", "localhost:9092")
          //
          //          props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
          //          props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
          //          props.put("group.id", "something")
          //          //          propc.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
          //          //          propc.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          //          //            "org.apache.kafka.common.serialization.StringDeserializer")
          //          //          propc.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          //          //            "org.apache.kafka.common.serialization.StringDeserializer")
          //          //           propc.put(("group.id", "something"))
          //          val consumer = new KafkaConsumer[String, String](props)
          //
          //          consumer.subscribe(kafkaOpTopic)
          //
          //          while (true) {
          //            val records = consumer.poll(100)
          //            for (record <- records.asScala) {
          //              println(record)
          //            }
          //          }

        })

      })

      ssc.remember(Minutes(1)) // To make sure data is not deleted by the time we query it interactively

      println("Creating function called to create new StreamingContext...!!!...")
      newContextCreated = true
      ssc
    }
    //    creatingFunc()
    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }

    val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

    ssc.start()
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)
  }
}



