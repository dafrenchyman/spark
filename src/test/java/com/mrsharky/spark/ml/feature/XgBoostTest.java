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

import com.mrsharky.scala.utilities.JavaScalaUtils;
import com.mrsharky.spark.utilities.SetupSparkTest;
import static com.mrsharky.spark.utilities.SparkUtils.CreateDefaultSparkSession;
import static com.mrsharky.spark.utilities.SparkUtils.PrintSparkSetting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.spark.api.python.PythonUtils.toScalaMap;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
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
        
        // Index labels, adding metadata to the label column.
        // Fit on whole dataset to include all labels in index.
        StringIndexer li = new StringIndexer()
            .setInputCol("label")
            .setOutputCol("indexedLabel");
        data = li.fit(data).transform(data);
        
        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexer vi = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .setMaxCategories(4);
        data = vi.fit(data).transform(data);
        
        //Map<String, Object> params = new HashMap<String, Object>();
        //params.put("num_rounds", 100);
        
        // XgBoost
        XGBoostEstimator xgb = new XGBoostEstimator(/*toScalaMap(params)*/)
                .setRound(10)
                .setSeed(1234L)
                .setBoosterType("gbtree")
                .setMaxDepth(4)
                .setNormalizeType("forest")
                .setFeaturesCol("indexedFeatures")
                .setPredictionCol("prediction")
                .setLabelCol("indexedLabel");
                  
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { xgb });
        
        // TODO: make it so you can save it without a fit
        // Save the pipeline without training it
        //pipeline.write().overwrite().save(outputModelLocation);
        //pipeline = Pipeline.load(outputModelLocation);
        
        PipelineModel model = pipeline.fit(data);
       
        // Save and load pipeline to disk
        // This needs to be done to save if Transformer process created correctly
        model.write().overwrite().save(outputModelLocation);
        PipelineModel model2 = PipelineModel.load(outputModelLocation);
        
        // transform the dataset and show results
        Dataset<Row> output = model2.transform(data);
        output.show();
        
        // Test cross-validation works
        List<String> boosterTypes = new ArrayList<String>();
        boosterTypes.add("gbtree");
        boosterTypes.add("gblinear");
        boosterTypes.add("dart");
        
        ParamMap[] paramGrid = new ParamGridBuilder()
                //.addGrid(xgb.boosterType(), JavaScalaUtils.JavaListToScalaIterable(boosterTypes) )
                .addGrid(xgb.round(), new int[]{1,2,3,4,5,6,7})
                .build();   
        
        CrossValidator crossVal = new CrossValidator()
            .setEstimator(pipeline)
            .setEvaluator(
                    new RegressionEvaluator()
                            .setPredictionCol("prediction")
                            .setLabelCol("indexedLabel") )
            .setEstimatorParamMaps(paramGrid).setNumFolds(2);
        CrossValidatorModel cvModel = crossVal.fit(data);
                
        Dataset<Row> results = cvModel.transform(data);
        results.show(1000);
        
        double[] avgMetrics = cvModel.avgMetrics();
        for (double currMet : avgMetrics) {
            System.out.println("Model: " + currMet);
        }
        
    }
}
