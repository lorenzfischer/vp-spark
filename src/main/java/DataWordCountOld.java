/* TODO: License */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * This app counts the number of words in all of the data pool.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 *
 * todo: redo this in scala: http://stackoverflow.com/questions/6758258/running-a-maven-scala-project
 */
public class DataWordCountOld {

    public static void main(String... args) {
        SparkConf sparkConf;
        JavaSparkContext spark;
        JavaRDD<String> inputFile;
        JavaRDD<String> words;
        JavaPairRDD<String, Integer> pairs;
        JavaPairRDD<String, Integer> counts;

        sparkConf = new SparkConf().setAppName("DataWordCount");
        sparkConf.set("akka.version","2.3.4");

        spark = new JavaSparkContext(sparkConf);

        inputFile = spark.textFile("/user/lfischer/voxpopuli/dewiki-latest-pages-articles.xml");
        words = inputFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
        });
        pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) { return new Tuple2<>(s, 1); }
        });
//        counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer a, Integer b) { return a + b; }
//        });

        Integer totalCount = pairs.values().reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        //counts.saveAsTextFile("/user/lfischer/voxpopuli/wordcounts");


    }
}
