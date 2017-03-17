import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by dbg on 16/03/2017.
 */
class SparkUtil {
    private SQLContext sqlContext;
    private JavaSparkContext sc;

    SparkUtil(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");
        // Create a Java version of the Spark Context from the configuration
         sc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(sc);
    }

    DataFrame readData(String filename)
    {
        // Define a configuration to use to interact with Spark
        return sqlContext.read().format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").option("inferSchema", "true").load(filename);
    }

    public SQLContext getSqlContext(){
        return sqlContext;
    }


    public DataFrame getTrainingData(){
        DataFrame dataframeCz = readData("src/main/resources/Train/raw_epochs_Cz.txt").withColumn("rowId",org.apache.spark.sql.functions.monotonically_increasing_id());
        DataFrame targets = readData("src/main/resources/Train/targets.txt").withColumn("rowId",org.apache.spark.sql.functions.monotonically_increasing_id());
        // register into sql engine
        dataframeCz.registerTempTable("Cz");
        targets.registerTempTable("targets");
        // merge the tables -- ugly, but by far the easiest way
        DataFrame trainingData = sqlContext.sql("select Cz.*, targets.C0 as label from Cz join targets on Cz.rowId = targets.rowId");
        //trainingData.show();
        trainingData = trainingData.drop("rowId");

        String[] data = Arrays.copyOfRange(trainingData.columns(), 0, trainingData.columns().length-2);

        VectorAssembler assembler = new VectorAssembler().setInputCols(data).setOutputCol("data");

        return assembler.transform(trainingData).select("data","label");
    }

    public DataFrame getTestingData(){
        DataFrame dataframeCz = readData("src/main/resources/Test/raw_epochs_Cz.txt").withColumn("rowId",org.apache.spark.sql.functions.monotonically_increasing_id());
        DataFrame targets = readData("src/main/resources/Test/targets.txt").withColumn("rowId",org.apache.spark.sql.functions.monotonically_increasing_id());
        // register into sql engine
        dataframeCz.registerTempTable("Cz");
        targets.registerTempTable("targets");
        // merge the tables -- ugly, but by far the easiest way
        DataFrame testData = sqlContext.sql("select Cz.*, targets.C0 as label from Cz join targets on Cz.rowId = targets.rowId");
        //trainingData.show();
        testData = testData.drop("rowId");

        String[] data = Arrays.copyOfRange(testData.columns(), 0, testData.columns().length-2);

        VectorAssembler assembler = new VectorAssembler().setInputCols(data).setOutputCol("data");

        return assembler.transform(testData).select("data","label");
    }

}
