/*
 * MIT License
 *
 * Copyright (c) 2018 Julien Pierret
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.mrsharky.spark.ml.feature;

import com.mrsharky.spark.utilities.SetupSparkTest;
import static com.mrsharky.spark.utilities.SparkUtils.CreateDefaultSparkSession;
import static com.mrsharky.spark.utilities.SparkUtils.PrintSparkSetting;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator;
import ml.dmlc.xgboost4j.scala.spark.XGBoostModel;
import static org.apache.spark.api.python.PythonUtils.toScalaMap;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import scala.collection.JavaConverters;

/**
 *
 * @author Julien Pierret
 */
public class XgBoostTest {
    
    private SetupSparkTest sparkSetup;
    private SparkSession spark;
  
    @BeforeClass
    public static void setupTests() throws Exception {
    }

    @Before
    public void setup() throws Exception {
        sparkSetup = new SetupSparkTest();
        sparkSetup.setup(2);
    }

    @After
    public void tearDown() {
        sparkSetup.tearDown();
    }
    
    public XgBoostTest() {
    }
    
    @Test
    public void testConcatColumns() throws IOException, Exception {
        
        String outputModelLocation = "./results/" + this.getClass().getSimpleName();
        spark = CreateDefaultSparkSession(this.getClass().getName());
        PrintSparkSetting(spark);
        
        // Load and parse the data file, converting it to a DataFrame.
        Dataset<Row> data = spark
                .read()
                .format("libsvm")
                .load("data/sample_libsvm_data.txt");
        
        // Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splits = data.randomSplit(new double[] {0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
        

        // Index labels, adding metadata to the label column.
        // Fit on whole dataset to include all labels in index.
        StringIndexerModel labelIndexer = new StringIndexer()
            .setInputCol("label")
            .setOutputCol("indexedLabel")
            .fit(data);
        trainingData = labelIndexer.transform(trainingData);
        
        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .setMaxCategories(4)
            .fit(data);
        trainingData = featureIndexer.transform(trainingData);

        // build a pipeline that concats two columns
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("num_rounds", 100);
        
                
        trainingData.show();
        
        // Spark Boosted Tree
        // Train a GBT model.
        GBTClassifier gbt = new GBTClassifier()
            .setLabelCol("indexedLabel")
            .setFeaturesCol("indexedFeatures")
            .setMaxIter(10);
        Dataset<Row> results0 = gbt.fit(trainingData).transform(trainingData);
        results0.show(100);
        
        
        com.mrsharky.spark.ml.feature.XGBoostEstimator xg1 = new com.mrsharky.spark.ml.feature.XGBoostEstimator(toScalaMap(params))
                .setFeaturesCol("indexedFeatures")
                .setLabelCol("indexedLabel");
        XGBoostModel xgm1 = xg1.fit(trainingData);
        Dataset<Row> results1 = xgm1.transform(trainingData);
        results1.show(100);
        
        com.mrsharky.spark.ml.feature.XGBoostEstimator xg2 = new com.mrsharky.spark.ml.feature.XGBoostEstimator(toScalaMap(params))
                .setRound(100)
                .setBoosterType("gbtree")
                .setMaxDepth(10)
                .setFeaturesCol("indexedFeatures")
                .setLabelCol("indexedLabel");
        XGBoostModel xgm2 = xg2.fit(trainingData);
        Dataset<Row> results2 = xgm2.transform(trainingData);
        results2.show(100);
                

        String boosterType = xg2.getBoosterType();
        
        TopCategories tc = new TopCategories()
                .setInputCols( new String[] {"StringColumn1", "StringColumn2"} )
                .setNumCategories(2);
        Pipeline pipeline = new Pipeline().setStages(
            new PipelineStage[] { tc });
        PipelineModel model = pipeline.fit(data);
       
        // Save and load pipeline to disk
        // This needs to be done to save if Transformer process created correctly
        model.write().overwrite().save(outputModelLocation);
        PipelineModel model2 = PipelineModel.load(outputModelLocation);
        
        // transform the dataset and show results
        Dataset<Row> output = model2.transform(data);
        output.show();
                
    }
}
