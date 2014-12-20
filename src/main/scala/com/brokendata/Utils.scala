package com.brokendata

/**
 * Created by ryan on 12/20/14.
 *
 * Helper object for authenticating to the Twitter API
 *
 */
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import sys.process.stringSeqToProcess
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object Utils {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.streaming.NetworkInputTracker").setLevel(Level.INFO)


  /** Configures the Oauth Credentials for accessing Twitter */


  def configureTwitterCredentials(path: String) {
    val file = new File(path)
    if (!file.exists) {
      throw new Exception("Could not find configuration file " + file)
    }
    val lines = Source.fromFile(file.toString).getLines.filter(_.trim.size > 0).toSeq
    val pairs = lines.map(line => {
      val splits = line.split("=")
      if (splits.size != 2) {
        throw new Exception("Error parsing configuration file - incorrectly formatted line [" + line + "]")
      }
      (splits(0).trim(), splits(1).trim())
    })

    pairs.map(x => {
      System.setProperty("twitter4j.oauth" + x._1, x._2)
    })
  }

    def getAuth = {
      Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
    }


}