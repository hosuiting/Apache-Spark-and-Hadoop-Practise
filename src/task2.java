import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class task2 {
	private static final Pattern SPACE = Pattern.compile("\\s+");
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    if (args.length < 1) {
	        System.err.println("Usage: task2 <file>");
	        System.exit(1);
	      }
	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .appName("task2")
	    	      .getOrCreate();
	    //read the text file
	    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
	    JavaRDD<String> words = lines.flatMap(s ->Arrays.asList(s.split(" ")[2].split(".")).iterator() );
	    
	    /*
	    JavaRDD<String> words = lines.flatMap(
	    		  new FlatMapFunction<String, String>() {
	    		    public Iterable<String> call(String s) {
	    		      return Arrays.asList(s.split(" "));
	    		    }
	    		  }
	    		);
	    
	    JavaRDD<String> words = lines.flatMap(
	    		  new FlatMapFunction<String, String>() {
	    		    public Iterable<String> call(String s) throws Exception  {
	    		      return Arrays.asList(s.split(" ").iterator());
	    		    }
	    		  }
	    		);
	    */
	    
	    //split the words accoirding to white space
	    //JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
	
	}

}
