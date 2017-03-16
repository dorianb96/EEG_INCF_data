import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by dbg on 16/03/2017.
 */
class SparkUtil {
    private SQLContext sqlContext;

    SparkUtil(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");
        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(sc);
    }

    DataFrame readData(String filename)
    {
        // Define a configuration to use to interact with Spark

        return sqlContext.read().format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").option("inferSchema", "true").load(filename);
    }

}
