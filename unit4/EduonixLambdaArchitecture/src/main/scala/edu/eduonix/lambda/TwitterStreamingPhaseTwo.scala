package edu.eduonix.lambda

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
 * https://github.com/javasoze
 *
 *     val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })
 */
object TwitterStreamingPhaseTwo {


  def main(args: Array[String]): Unit = {


    System.setProperty("twitter4j.oauth.consumerKey", "aGXWJ47fhTrGZi4mcuW0ITj3Z")
    System.setProperty("twitter4j.oauth.consumerSecret", "FEgb5An1qyS5GZqfbTtoe0ScVSph7cDCSJuLogGqJQqhbJDEko")
    System.setProperty("twitter4j.oauth.accessToken", "765741703588253698-EI4SLmxj7Q3AXj25N7BYgY9MbVg8Sjw")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "73N9R78ypQynbFnINIbdT33vtTnliQvI3SWhtLeVWmBSX")


    val filters = Array("#IPL")


    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)
//    val stream = TwitterUtils.createStream(ssc, None)
//    stream.print()

    val hashTags = stream.map(status => status.getText.split(" ").filter(_.startsWith("#")))
    hashTags.print()

    ssc.start()
    ssc.awaitTermination()
  }






}
