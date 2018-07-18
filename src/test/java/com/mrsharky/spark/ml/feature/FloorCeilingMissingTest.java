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
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Julien Pierret
 */
public class FloorCeilingMissingTest {
    
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
    
    public FloorCeilingMissingTest() {
    }
    
    @Test
    public void testConcatColumns() throws IOException {
        
        String outputModelLocation = "./results/" + this.getClass().getSimpleName();
        spark = CreateDefaultSparkSession(this.getClass().getName());
        PrintSparkSetting(spark);
        
        Dataset<Row> data = spark.read().format("csv").option("header", true).load("./data/testData.csv");
        data = data.repartition(10);
        
        
        data = data.select("Numeric1", "Numeric2", "Numeric3");
        
        data = data.withColumn("Numeric1_tmp", col("Numeric1").cast(DoubleType))
                .drop("Numeric1").withColumnRenamed("Numeric1_tmp", "Numeric1");
        data = data.withColumn("Numeric2_tmp", col("Numeric2").cast(IntegerType))
                .drop("Numeric2").withColumnRenamed("Numeric2_tmp", "Numeric2");
        data = data.withColumn("Numeric3_tmp", col("Numeric3").cast(LongType))
                .drop("Numeric3").withColumnRenamed("Numeric3_tmp", "Numeric3");
        
        // Show the input data
        data.show();
        
        // build a pipeline that concats two columns
        FloorCeilingMissing fc = new FloorCeilingMissing()
                .setInputCols( new String[] {"Numeric1", "Numeric2", "Numeric3"} )
                //.setLowerPercentile(0.10)
                .setUpperPercentile(0.90);
                //.setMissingPercentile(0.50);
        Pipeline pipeline = new Pipeline().setStages( new PipelineStage[] { fc });
        
        // Save the pipeline without training it
        pipeline.write().overwrite().save(outputModelLocation);
        pipeline = Pipeline.load(outputModelLocation);
        
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
