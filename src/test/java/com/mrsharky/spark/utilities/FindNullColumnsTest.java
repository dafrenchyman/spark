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
package com.mrsharky.spark.utilities;

import com.mrsharky.spark.functions.FindNullColumns;
import com.mrsharky.spark.ml.feature.DropColumns;
import static com.mrsharky.spark.utilities.SparkUtils.CreateDefaultSparkSession;
import static com.mrsharky.spark.utilities.SparkUtils.PrintSparkSetting;
import java.util.List;
import junit.framework.Assert;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Column;
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
public class FindNullColumnsTest {
    
    public FindNullColumnsTest() {
    }
    
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

    /**
     * Test of getNullColumnsAsListString method, of class FindNullColumns.
     */
    @Test
    public void testFindNullColumns() {
        
        spark = CreateDefaultSparkSession(this.getClass().getName());
        PrintSparkSetting(spark);
        
        Dataset<Row> data = spark.read().format("csv").option("header", true).load("./data/testData.csv");
        
        // Show the input data
        data.show();
        
        FindNullColumns fnc = new FindNullColumns(data);
        
        List<String> nullColumns = fnc.getNullColumnsAsListString();
        for (String nullColumn : nullColumns) {
            if (!nullColumn.contains("NullColumn")) {
                Assert.fail(nullColumn + " is not a valid null column");
            } else {
                System.out.println(nullColumn + " valid null column");
            }
        }
    }    
}
