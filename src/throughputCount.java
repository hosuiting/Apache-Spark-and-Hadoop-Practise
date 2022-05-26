import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class throughputCount {
	public static void main(String[] args) {
		 SparkConf sparkConf = new SparkConf().setAppName("throughputCount").setMaster("local[*]");
		 JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		 JavaRDD<String> data = jsc.textFile(args[0],1);
		 
		 // break the line according to white space
	     JavaRDD<String> wordsJavaRDD = data.flatMap(
	    		 new FlatMapFunction<String, String>() {
	    	 public Iterator<String> call(String line) throws Exception {
	                String[] words = line.split("\\s+");
	                // break the url according to . and obtain token
	                //2022-05-21 19:48:25 INFO   : CreateAccount(*) success, latency: 1635ms, id:1, client:1 
	                String[] time = words[1].split(":");
	                String tmp;
	                tmp = time[0] + ":" + time[1]+ ":" + time[2];
	                //System.out.println(tmp);
	                String[] buff = new String [] {tmp};
	                return Arrays.asList(buff).iterator();
	            }
	        });
	     // map the word as key and place 1
	     JavaPairRDD<String, Integer> wordAndOne = wordsJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
	            public Tuple2<String, Integer> call(String word) throws Exception {
	                return new Tuple2<String, Integer>(word, 1);
	            }
	        });
	     //reduce
	     JavaPairRDD<String, Integer> result = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
	            public Integer call(Integer v1, Integer v2) {
	                return v1 + v2;
	            }
	        });
	     List<Tuple2<String, Integer>> finalResult = result.collect();
	     // find the average throughput
	     int totalThroughPut = 0;
	     int count = 0;
	     for (Tuple2<String, Integer> t: finalResult) {
	        		System.out.println("TimePeriod:" + t._1 + "\t Number of Times ："+ t._2);
	        		totalThroughPut += (int)t._2;
	        		count ++;
	     }
	     System.out.println("Average Throughput = " + totalThroughPut/count);
	     jsc.stop();
	}
}
