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

public class TimeCount {
	 public static void main(String[] args) {
		 SparkConf sparkConf = new SparkConf().setAppName("TimeCount").setMaster("local[*]");
		 JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		 JavaRDD<String> data = jsc.textFile(args[0],1);

	     // break the line according to white space
	     JavaRDD<String> wordsJavaRDD = data.flatMap(
	    		 new FlatMapFunction<String, String>() {
	    	 public Iterator<String> call(String line) throws Exception {
	                String[] words = line.split("\\s+");
	                // break the url according to . and obtain token
	                String[] time = words[0].split(":");
	                String tmp;
	                if(words[0].equals("queryTime")){
	                	tmp = "0";
	                }else {
	                	tmp = "0" + time[0] + ":" + time[1];
	                }
	                //System.out.println(tmp);
	                String[] buff = new String [] {tmp};
	                return Arrays.asList(buff).iterator();
	            }
	        });
/*
	     // break the url according to . and obtain token
	     JavaRDD<String> wordsJavaRDD2 = wordsJavaRDD.flatMap(new FlatMapFunction<String, String>() {
	    	 public Iterator<String> call(String line) throws Exception {
	                String[] words = line.split("\\.");
	                return Arrays.asList(words).iterator();
	            }
	        });
*/
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
	     
	     //sort by key
	     JavaPairRDD<Integer, String> reverseJavaPairRDD = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
	                return new Tuple2<Integer, String>(t._2, t._1);
	            }
	        });
	     
	     JavaPairRDD<String, Integer> sortedRDD = reverseJavaPairRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
	            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
	                return new Tuple2<String, Integer>(t._2, t._1);
	            }
	        });
	     List<Tuple2<String, Integer>> finalResult = sortedRDD.collect();
	     	int count = 0;
	        for (Tuple2<String, Integer> t: finalResult) {
	        	if(count<10) {
	        		System.out.println("Words:" + t._1 + "\t Times ："+ t._2);
	        		count ++;
	        	}
	        }
	     jsc.stop();
	 }
}
