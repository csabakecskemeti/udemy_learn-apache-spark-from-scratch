package edu.eduonix.lambda;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by ubu on 19.12.14.
 */
public class TwitterStreamProcessor {

    static boolean runOnCluster = false;
    static AtomicLong tweetCounter = new AtomicLong(0);
    static Gson gson = new Gson();
    public static void main(String[] args) {

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterStreamProcessor");

        if (!runOnCluster) {

            sparkConf.setMaster("local[4]");
            sparkConf.setJars(new String[]{"target/lambda-deploy.jar"});
        } else {

        }


        try {
            configureTwitterCredentials();
        } catch (Exception e) {
            e.printStackTrace();
        }

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        String[] filters = new String[]{"#IPL"};


        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(ssc, filters);

        JavaDStream<String> statuses = tweets.map(
                new Function<Status, String>() {

                    public String call(Status status) {

                        String s = status.getText();
                        return s;
                    }
                }
        );
        statuses.print();

        JavaReceiverInputDStream<Status> tweetsUnFiltered = TwitterUtils.createStream(ssc);
//        JavaDStream<String> mapped = tweetsUnFiltered.map(
//                new Function<Status, String>() {
//
//                    public String call(Status status) {
//
//                        String s = status.getText();
//                        System.out.println(gson.toJson(status));
////                        System.out.println("cnt>>> " + tweetCounter.get());
//                        return gson.toJson(status);
//                    }
//                }
//        );
        JavaDStream<String> mapped = tweetsUnFiltered.map((status) -> {
                System.out.println(gson.toJson(status));
                System.out.println("cnt>>> " + tweetCounter.get());
                return gson.toJson(status);
        });


//        mapped.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
//            @Override
//            public Void call(JavaRDD<String> rdd, Time time)
//                    throws Exception {
//                tweetCounter.addAndGet(rdd.count());
//                rdd.saveAsTextFile("./target/stream/"+File.separator+tweetCounter.get());
//                return null;
//            }
//        });
        mapped.foreachRDD((rdd) -> {
            tweetCounter.addAndGet(rdd.count());
            rdd.saveAsTextFile("./target/stream/"+File.separator+tweetCounter.get());
            return null;
        });

//        tweetsUnFiltered.print();
        ssc.start();
        ssc.awaitTermination();
        System.out.println(">>> msg cnt: " + tweetCounter.get());


    }


    static void configureTwitterCredentials() throws Exception {
        File file = new File("twitter.txt");
        if (!file.exists()) {
            throw new Exception("Could not find configuration file " + file);
        }
        List<String> lines = readLines(file);
        HashMap<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] splits = line.split("=");
            if (splits.length != 2) {
                throw new Exception("Error parsing configuration file - incorrectly formatted line [" + line + "]");
            }
            map.put(splits[0].trim(), splits[1].trim());
        }
        String[] configKeys = {"consumerKey", "consumerSecret", "accessToken", "accessTokenSecret"};
        for (int k = 0; k < configKeys.length; k++) {
            String key = configKeys[k];
            String value = map.get(key);
            if (value == null) {
                throw new Exception("Error setting OAuth authentication - value for " + key + " not found");
            } else if (value.length() == 0) {
                throw new Exception("Error setting OAuth authentication - value for " + key + " is empty");
            }
            String fullKey = "twitter4j.oauth." + key;
            System.setProperty(fullKey, value);
            System.out.println("\tProperty " + fullKey + " set as " + value);
        }
        System.out.println();
    }

    private static List<String> readLines(File file) throws IOException {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        List<String> lines = new ArrayList<String>();
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.length() > 0) lines.add(line);
        }
        bufferedReader.close();
        return lines;
    }
}
