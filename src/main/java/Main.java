import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;

/**
 * Sample Spark application that counts the words in a text file
 */
public class Main
{

    public static void main( String[] args )
    {

        SparkUtil spark = new SparkUtil();

        if( args.length == 0 )
        {
            System.out.println( "Usage: <file>" );
            System.exit( 0 );
        }

        DataFrame dataframeCz = spark.readData("src/main/resources/raw_epochs_Cz.txt" );
        DataFrame dataframeFz = spark.readData("src/main/resources/raw_epochs_Fz.txt" );
        DataFrame dataframeO1 = spark.readData("src/main/resources/raw_epochs_O1.txt" );
        DataFrame dataframePz = spark.readData("src/main/resources/raw_epochs_Pz.txt" );
        DataFrame targets = spark.readData("src/main/resources/targets.txt" );

        ArrayList<DataFrame> dataframes = new ArrayList<>(10);
        dataframes.add(dataframeCz);
        dataframes.add(dataframeFz);
        dataframes.add(dataframePz);
        dataframes.add(dataframeO1);
        dataframes.add(targets);

        for (DataFrame d : dataframes){
            System.out.println(d.count() +  " " + d.columns().length);
        }

    }
}
