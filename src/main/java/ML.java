import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.sql.DataFrame;

/**
 * Created by dbg on 16/03/2017.
 */
public class ML {

    SparkUtil sparkUtil;

    public ML(){
        sparkUtil = new SparkUtil();
    }

    public float trainLogisticRegression() {
        DataFrame trainingData = sparkUtil.getTrainingData();
        DataFrame testData = sparkUtil.getTestingData();

        MinMaxScaler scaler = new MinMaxScaler()
                .setInputCol("data")
                .setOutputCol("features");

        trainingData = scaler.fit(trainingData).transform(trainingData);
        testData=scaler.fit(testData).transform(testData);

        LogisticRegression lr = new LogisticRegression()
                .setLabelCol("label")
                .setFeaturesCol("features")
                .setMaxIter(100)
                .setRegParam(0.01);

        // Fit the pipeline to training documents.
        LogisticRegressionModel model = lr.fit(trainingData);

        DataFrame predictions = model.transform(testData);


        // Select (prediction, true label) and compute test error
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("precision");

        float accuracy = (float) evaluator.evaluate(predictions);

        return accuracy;

    }

}
