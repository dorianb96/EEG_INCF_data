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
