/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.net.URL;
import java.util.regex.Pattern;

/**
 * Example using MLlib KMeans from Java.
 */
public final class JavaKMeans {

  private static class ParsePoint implements Function<String, Vector> {
    private static final Pattern SPACE = Pattern.compile(" ");

    @Override
    public Vector call(String line) {
      String[] tok = SPACE.split(line);
      double[] point = new double[tok.length];
      for (int i = 0; i < tok.length; ++i) {
        point[i] = Double.parseDouble(tok[i]);
      }
      return Vectors.dense(point);
    }

  }


  static boolean runOnCluster = false;

  public static void main(String[] args) {

    URL resource= null;

    if(!runOnCluster) {
      resource = JavaKMeans.class.getResource("input_k_small.txt");
    }
    else if (args.length < 3) {
      System.err.println(
              "Usage: JavaKMeans <input_file> <k> <max_iterations> [<runs>]");
      System.exit(1);
    }

    String inputFile = null;
    int k = 0;
    int iterations = 0;
    int runs = 0;

    if(!runOnCluster) {

      inputFile = resource.getFile();
      System.out.println(resource.getFile());
      k = 10;
      iterations = 10;
      runs = 1;
    } else {

      inputFile  = args[0];
      k =   Integer.parseInt(args[1]);
      iterations = Integer.parseInt(args[2]);
      runs = 1;
      if (args.length >= 4) {
        runs = Integer.parseInt(args[3]);
      }

    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaKMeans");

    if(!runOnCluster) {

      sparkConf.setMaster("local");

    }

    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = sc.textFile(inputFile);

    JavaRDD<Vector> points = lines.map(new ParsePoint());

    KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs, KMeans.K_MEANS_PARALLEL());

    System.out.println("Cluster centers:");
    for (Vector center : model.clusterCenters()) {
      System.out.println(" " + center);
    }
    double cost = model.computeCost(points.rdd());
    System.out.println("Cost: " + cost);

    sc.stop();
  }
}