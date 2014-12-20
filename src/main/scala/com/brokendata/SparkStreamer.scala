package com.brokendata

import com.brokendata.Utils._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by ryan on 12/20/14.
 */
object SparkStreamer {

  def main(args: Array[String]) {

    // Initilize twitter oAuth
    configureTwitterCredentials("main/twitter.txt")

    // Initialize Spark Context

    println("Initiallizing Spark Context")

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))


    // TwitterUtils can accept a filter argument for filtering tweet content
    val filters = Array("Neiman Marcus")
    val stream = TwitterUtils.createStream(ssc, Utils.getAuth, filters)

    stream.print()

    ssc.start()
    ssc.awaitTermination()



  }




}
