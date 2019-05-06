import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Exercise_1 {
	
	public static String basicAnalysis(JavaSparkContext ctx) {
		String out = "";
		
		JavaRDD<String> adultRDD = ctx.textFile("src/main/resources/adult.1000.csv");

		out += "The file has "+adultRDD.count()+" lines\n";
		out += "#################################\n";
		
		out += "The first five lines have the following content:\n";
		for (String line : adultRDD.take(5)) {
			out += "	"+line;
			out += "\n";
		}
		out += "#################################\n";

		JavaRDD<Integer> thirdAttribute = adultRDD.map(f -> Integer.parseInt(f.split(",")[2])).sortBy(f -> f,true,2);
		out += "For the third attribute the minimum value is "+thirdAttribute.first()+"\n";
		out += "#################################\n";

		JavaRDD<String> type1 = adultRDD.filter(f -> f.contains("Doctorate"));
		out += type1.count()+" adults have a doctorate\n";
		out += "#################################\n";
		
		return out;
	}
}

