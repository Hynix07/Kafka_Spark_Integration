package spark_kafka;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class sparkConnection {

    //setup the private variables
    private static SparkConf conf = null;
    private static JavaSparkContext jc = null;
    private static  JavaStreamingContext jssc = null;
    private static String Master = "local[2]";
    private static String AppName = "my-app";

    private static void getConnection(){
        if(conf==null){
            conf = new SparkConf().setAppName(AppName).setMaster(Master);
        }
    }

    // setup public functions to get spark connection
    public static JavaSparkContext getContext(){
        if(jc==null){
            getConnection();
            jc = new JavaSparkContext(conf);
        }
        return jc;
    }
    public static JavaStreamingContext getStreamConnection(int time){
        if(jssc==null){
            getConnection();
            jssc = new JavaStreamingContext(conf, Durations.seconds(time));
        }
        return jssc;
    }
}
