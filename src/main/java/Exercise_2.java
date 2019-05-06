import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Exercise_2 {

    public static String groupByAndAgg(JavaSparkContext ctx) {
        String out = "";

        JavaRDD<String> adultRDD = ctx.textFile("src/main/resources/adult.1000.csv");

        List<Tuple2<String,Double>> vers1 = adultRDD
                .mapToPair(f -> new Tuple2<String, Double>(f.split(",")[12],Double.parseDouble(f.split(",")[9])))
                .reduceByKey((f1,f2) -> f1+f2)
                .collect();

        out += "groupByAndAggregation\n";
        out += Arrays.toString(vers1.toArray());

        return out;
    }

}
