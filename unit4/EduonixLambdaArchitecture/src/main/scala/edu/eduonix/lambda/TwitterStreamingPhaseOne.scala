package edu.eduonix.lambda

import java.io.{InputStreamReader, BufferedReader}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.{SparkConf, Logging}

import java.net.Socket
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.receiver.Receiver

/**
 * https://chimpler.wordpress.com/2014/07/01/implementing-a-real-time-data-pipeline-with-spark-streaming/
 *
 * https://github.com/apache/spark/blob/master/examples/scala-2.10/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala
 */
object TwitterStreamingPhaseOne {

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext("local[4]", "TwitterStreamingPhaseOne", Seconds(10))

  // val lines = ssc.receiverStream(new CustomReceiver("localhost", 1111))
     val lines =  ssc.socketTextStream("localhost", 1111, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    words.print()
    ssc.start()
    ssc.awaitTermination()

  }

  class CustomReceiver(host: String, port: Int)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

    def onStart() {
      // Start the thread that receives data over a connection
      new Thread("Socket Receiver") {
        override def run() { receive() }
      }.start()
    }

    def onStop() {
      // There is nothing much to do as the thread calling receive()
      // is designed to stop by itself isStopped() returns false
    }

    /** Create a socket connection and receive data until receiver is stopped */
    private def receive() {
      var socket: Socket = null
      var userInput: String = null
      try {
        println  ("Connecting to " + host + ":" + port)
        socket = new Socket(host, port)
        println("Connected to " + host + ":" + port)
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
        userInput = reader.readLine()
        while(!isStopped && userInput != null) {
          store(userInput)
          userInput = reader.readLine()
        }
        reader.close()
        socket.close()
        println("Stopped receiving")
        restart("Trying to connect again")
      } catch {
        case e: java.net.ConnectException =>
          restart("Error connecting to " + host + ":" + port, e)
        case t: Throwable =>
          restart("Error receiving data", t)
      }
    }
  }

}
