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

public class latency {
	public static void main(String[] args) {
		 SparkConf sparkConf = new SparkConf().setAppName("latency").setMaster("local[*]");
		 JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		 JavaRDD<String> data = jsc.textFile(args[0],1);
		 
		 JavaPairRDD<String, Long> data2 = data.mapToPair(line -> { 
			//2022-05-21 19:48:25 INFO   : CreateAccount(*) success, latency: 1635ms, id:1, client:1 
				String[] a = line.split("latency: ");
				String[] b = a[1].split("ms");
				//b[0] store 1635
				String c[] = line.split("\\(\\*");
				String d[] = c[0].split("INFO   : ");
				//d[1] store CreateAccount
				return new Tuple2<String, Long>(d[1], Long.parseLong(b[0]));
			});
		 JavaPairRDD<String, Tuple2<Long, Long>> data3 = data2.mapValues(x -> new Tuple2<Long, Long>(x, 1L));
		 JavaPairRDD<String, Tuple2<Long, Long>> data4 = data3.reduceByKey((x, y) -> {
				return new Tuple2<Long, Long>(x._1 + y._1, x._2 + y._2);
			});
		 JavaPairRDD<String, Double> data5 = data4.mapToPair(x -> {
				return new Tuple2<String, Double>(x._1, x._2._1 * 1.0D / x._2._2);
			});
	     List<Tuple2<String, Double>> finalResult = data5.collect();
	     // find the average throughput
	     for (Tuple2<String, Double> t: finalResult) {
	        		System.out.println("API Call:" + t._1 + "\t Average latency："+ t._2);
	     }
	}
}
