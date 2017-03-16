import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.DataFrame;
import org.apache.commons.lang3.tuple.Pair;
import java.util.ArrayList;

/**
 * Sample Spark application that counts the words in a text file
 */
public class Main
{

    public static void main( String[] args ) {

        SparkUtil spark = new SparkUtil();

        DataFrame dataframeCz = spark.readData("src/main/resources/raw_epochs_Cz.txt");
        DataFrame dataframeFz = spark.readData("src/main/resources/raw_epochs_Fz.txt");
        DataFrame dataframeO1 = spark.readData("src/main/resources/raw_epochs_O1.txt");
        DataFrame dataframePz = spark.readData("src/main/resources/raw_epochs_Pz.txt");
        DataFrame targets = spark.readData("src/main/resources/targets.txt");

        ArrayList<DataFrame> dataframes = new ArrayList<>(10);
        dataframes.add(dataframeCz);
        dataframes.add(dataframeFz);
        dataframes.add(dataframePz);
        dataframes.add(dataframeO1);
        dataframes.add(targets);

        ArrayList<Pair<Long, Integer>> dimensions = new ArrayList<>();

        for (DataFrame d : dataframes) {
            dimensions.add(new ImmutablePair<>(d.count(), d.columns().length));
//            System.out.println(d.count() + " " + d.columns().length);
        }
        System.out.println(dimensions);

    }
}
