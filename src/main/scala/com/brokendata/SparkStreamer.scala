package com.brokendata
/*
import com.brokendata.Utils
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext._
*/
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ryan on 12/20/14.
 */
object SparkStreamer {

  def main(args: Array[String]) {

    // Initilize twitter oAuth
    Utils.configureTwitterCredentials("src/main/twitter.txt")
    Utils.setStreamingLogLevels()
    // Initialize Spark Context

    println("Initiallizing Spark Context")

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext("local[2]", this.getClass.getSimpleName)
    val ssc = new StreamingContext(sc, Seconds(5))


    // TwitterUtils can accept a filter argument for filtering tweet content
    val filters = Array("")
    val stream = TwitterUtils.createStream(ssc, Utils.getAuth, filters)

    val status = stream.map(_.getText)

    status.foreachRDD(rdd => rdd.take(10).foreach(println))

    ssc.start()
    ssc.awaitTermination()

    /*
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

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

    ssc.start()
    ssc.awaitTermination()
    */

  }




}
