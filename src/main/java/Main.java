import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.DataFrame;
import org.apache.commons.lang3.tuple.Pair;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Sample Spark application that counts the words in a text file
 */
public class Main
{

    public static void main( String[] args ) {

        ML machineLearning = new ML();
        float logRegAccu = machineLearning.trainLogisticRegression();

        System.out.println("Logistic regression accuracy = " + logRegAccu);

    }
}
