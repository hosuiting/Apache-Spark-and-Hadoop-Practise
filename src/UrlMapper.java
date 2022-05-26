import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class UrlMapper extends Mapper <LongWritable, Text, Text, IntWritable>{
	private Text wordToken = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer tokens = new StringTokenizer(value.toString(),"\n"); //Dividing String into tokens by each line
		while (tokens.hasMoreTokens()){
			String temp = tokens.nextToken();
			String[] buff = temp.split("/");
			//wordToken.set(tokens.nextToken());
			wordToken.set(buff[0]);
			context.write(wordToken, new IntWritable(1));
		}
	}
}
