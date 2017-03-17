# Distributed Machine Learning on EEG data using Apache Spark

### A protoype of distributed computing engine for processing of EEG data

### The application has the basic machine learning process of 
 1. Loading the data (in this case from local file system not hadoop)
 2. Feature extraction (here we just scale all metrics to [0,1] range)
 3. Train the classifier (we train a logistic regression classifier)

### Further improvments :
1. Load more data
2. Add better signal processing methods
3. Improve feature extraction process
4. Add many other classifiers such as mentioned here: https://spark.apache.org/docs/latest/ml-classification-regression.html
5. Present better the metrics of a model (accuracy, RoC …)
6. Management of classifiers ie that you can load them from files
7. Design easy application management with arguments such as input file location, parameters which signal processing methods to use or which classifier to use, where to save results, what metrics to track…

### Running the application

Probably the easiest way to load this application is just to clone or check it out from Github. 
It will run even without Apache Spark or Hadoop installed.
Using IntellijIdea it's really easy to set-it up, I don't know for Eclipse.


