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
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
public class DropColumnsTest {
    
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
    
    public DropColumnsTest() {
    }
    
    @Test
    public void testConcatColumns() throws IOException {
        
        String outputModelLocation = "./results/" + this.getClass().getSimpleName();
        spark = CreateDefaultSparkSession(this.getClass().getName());
        PrintSparkSetting(spark);
        
        Dataset<Row> data = spark.read().format("csv").option("header", true).load("./data/testData.csv");
        
        // Show the input data
        data.show();
        
        // build a pipeline that concats two columns
        DropColumns dc = new DropColumns()
                .setInputCols( new String[] {"StringColumn1", "StringColumn2"} );
        Pipeline pipeline = new Pipeline().setStages( new PipelineStage[] { dc });
        
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
