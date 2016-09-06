/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.eduonix.lambda

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object ConfigCheck {
  def main(args: Array[String]) {

    System.setProperty("twitter4j.oauth.consumerKey", "aGXWJ47fhTrGZi4mcuW0ITj3Z")
    System.setProperty("twitter4j.oauth.consumerSecret", "FEgb5An1qyS5GZqfbTtoe0ScVSph7cDCSJuLogGqJQqhbJDEko")
    System.setProperty("twitter4j.oauth.accessToken", "765741703588253698-EI4SLmxj7Q3AXj25N7BYgY9MbVg8Sjw")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "73N9R78ypQynbFnINIbdT33vtTnliQvI3SWhtLeVWmBSX")


    val conf = new SparkConf().setAppName("NetworkWordCount")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)


    val configKeys = List("consumerKey", "consumerSecret", "accessToken", "accessTokenSecret")


    println(sys.props.get("twitter4j.oauth.consumerKey"))
    println(sys.props.get("twitter4j.oauth.consumerSecret"))
    println(sys.props.get("twitter4j.oauth.accessToken"))
    println(sys.props.get("twitter4j.oauth.accessTokenSecret"))

  }
}
