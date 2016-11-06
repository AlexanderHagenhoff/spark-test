package de.sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main
{
    public static void main(String[] args)
    {
        new Main();
    }

    private Main()
    {
        SparkConf sparkConf = new SparkConf().setAppName("spark test").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("src/main/resources/test_ver2.csv");
        JavaRDD<Integer> lineLengths = lines.map(String::length);
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println("totalLength: "+totalLength);
    }
}
