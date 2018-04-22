/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
 * @author mrsharky
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
        
        String outputModelLocation = "./data/concatColumnsTest";
        spark = CreateDefaultSparkSession(this.getClass().getName());
        PrintSparkSetting(spark);
        
        Dataset<Row> data = spark.read().format("csv").option("header", true).load("./data/testData.csv");
        
        // Show the input data
        data.show();
        
        // build a pipeline that concats two columns
        DropColumns dc = new DropColumns()
                .setInputCols( new String[] {"StringColumn1", "StringColumn2"} );
        Pipeline pipeline = new Pipeline().setStages(
            new PipelineStage[] { dc });
        PipelineModel model = pipeline.fit(data);
       
        // Save and load pipeline to disk
        // This needs to be done to save if Transformer process created correctly
        model.write().overwrite().save(outputModelLocation);
        model = PipelineModel.load(outputModelLocation);
        
        // transform the dataset and show results
        Dataset<Row> output = model.transform(data);
        output.show();
        
    }
}
