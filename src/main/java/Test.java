import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.DataFrame;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

/**
 * Created by dbg on 16/03/2017.
 */
public class Test {

    @org.junit.Test
    public void test(){
        SparkUtil spark = new SparkUtil();

        DataFrame dataframeCz = spark.readData("src/main/resources/Train/raw_epochs_Cz.txt");
        DataFrame dataframeFz = spark.readData("src/main/resources/Train/raw_epochs_Fz.txt");
        DataFrame dataframeO1 = spark.readData("src/main/resources/Train/raw_epochs_O1.txt");
        DataFrame dataframePz = spark.readData("src/main/resources/Train/raw_epochs_Pz.txt");
        DataFrame targets = spark.readData("src/main/resources/Train/targets.txt");

        DataFrame dataframeCzTest = spark.readData("src/main/resources/Test/raw_epochs_Cz.txt");
        DataFrame dataframeFzTest = spark.readData("src/main/resources/Test/raw_epochs_Fz.txt");
        DataFrame dataframeO1Test = spark.readData("src/main/resources/Test/raw_epochs_O1.txt");
        DataFrame dataframePzTest = spark.readData("src/main/resources/Test/raw_epochs_Pz.txt");
        DataFrame targetsTest = spark.readData("src/main/resources/Test/targets.txt");

        ArrayList<DataFrame> dataframes = new ArrayList<>(5);
        ArrayList<DataFrame> testDataframes = new ArrayList<>(5);

        dataframes.add(dataframeCz);
        dataframes.add(dataframeFz);
        dataframes.add(dataframePz);
        dataframes.add(dataframeO1);
        dataframes.add(targets);
        testDataframes.add(dataframeCzTest);
        testDataframes.add(dataframeFzTest);
        testDataframes.add(dataframePzTest);
        testDataframes.add(dataframeO1Test);
        testDataframes.add(targetsTest);

        ArrayList<Pair<Long, Integer>> dimensions = new ArrayList<>();
        ArrayList<Pair<Long, Integer>> dimensionsTest = new ArrayList<>();

        for (DataFrame d : dataframes) {
            dimensions.add(new ImmutablePair<>(d.count(), d.columns().length));
//            System.out.println(d.count() + " " + d.columns().length);
        }

        for (DataFrame d : testDataframes) {
            dimensionsTest.add(new ImmutablePair<>(d.count(), d.columns().length));
//            System.out.println(d.count() + " " + d.columns().length);
        }

        // first element
        for (int i =0; i < 5; i++){
            assertEquals(dimensions.get(i).getLeft(),dimensions.get(1).getLeft());
        }
        for (int i =0; i < 3; i++){
            assertEquals(dimensionsTest.get(i).getLeft(),dimensionsTest.get(i+1).getLeft());
        }
        // second element
        for (int i =0; i < 2; i++){
            assertEquals(dimensions.get(i).getRight(),dimensions.get(1).getRight());
        }
        for (int i =0; i < 2; i++){
            assertEquals(dimensionsTest.get(i).getRight(),dimensionsTest.get(i+1).getRight());
        }
        // only one column in labels
        int j = 1;
        assertTrue(dimensions.get(4).getRight() == 1);

        assertTrue(dimensionsTest.get(4).getRight() == 1);

        System.out.println("Size of train data" + dimensions);
        System.out.println("Size of test data" + dimensionsTest);
        System.out.println("Tests passed");
    }
}
