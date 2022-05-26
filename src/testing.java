import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class testing {
	private static final Pattern SPACE = Pattern.compile("\\s+");
	private static final Pattern SPACE2 = Pattern.compile(".");
	public static void main(String[] args) {
		String abc = "2022-05-21 19:48:25 INFO   : CreateAccount(*) success, latency: 1635ms, id:1, client:1";
		String[] a = abc.split("latency: ");
		System.out.println(a[1]);
		String[] b = a[1].split("ms");
		System.out.println(b[0]);
		System.out.println(Long.parseLong(b[0]));
		String c[] = abc.split("\\(\\*");
		System.out.println(c[0]);
		String d[] = c[0].split("INFO   : ");
		System.out.println(d[1]);
		//String s = SPACE2.split(SPACE.split(abc));
		//String[]buffer = abc.split("\\s+");
		//System.out.println(buffer[0]);
		//String[]buffer2 = buffer[0].split(":");
		//System.out.println(buffer2[1]);
		//String tmp = "0" + buffer2[0] + ":"+buffer2[1];
		//System.out.println(tmp);
		//String[] buff = new String [] {tmp};
		//System.out.println(buff[0]);
	}
}
