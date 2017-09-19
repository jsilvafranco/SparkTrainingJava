package net.vpc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.Comparator;
import java.util.List;

public class RDDBatch {

    public void start() {
        SparkConf config = new SparkConf()
                .setAppName("rddBatch")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(config);
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("movies.csv");
        JavaRDD<String> logs = sc.textFile(new File(resource.getFile()).getPath());
        JavaPairRDD<String, Integer> map = logs.mapToPair(line -> {
            String[] data = line.split(",");
            return new Tuple2<>(data[0], 1);
        });

        JavaPairRDD<Integer, String> reduce = map.reduceByKey((a, b) -> a + b)
                                                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                                                .sortByKey(false);
        //List<Tuple2<String, Integer>> result = reduce.takeOrdered(10);
        List<Tuple2<Integer, String>> result = reduce.take(10);


        for (Tuple2<Integer, String> t : result) {
            System.out.println(t._1 + ":" + t._2);
        }
        sc.stop();

    }

    public static void main(String[] args){
        new  RDDBatch().start();
    }

}