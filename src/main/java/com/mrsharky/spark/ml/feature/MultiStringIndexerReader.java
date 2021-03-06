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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ml.util.DefaultParamsReader;
import org.apache.spark.ml.util.DefaultParamsReader$;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Julien Pierret
 */
public class MultiStringIndexerReader extends MLReader<MultiStringIndexer> implements Serializable {
    
    private String className = MultiStringIndexer.class.getName();
    
    public String getClassName() {
        return className;
    }
    
    public void setClassName(String className) {
        this.className = className;
    }
    
    @Override
    public MultiStringIndexer load(String path) {
        DefaultParamsReader.Metadata metadata = DefaultParamsReader$.MODULE$.loadMetadata(path, sc(), className);
        String dataPath = new Path(path, "data").toString();
        Dataset<Row> data = sparkSession().read().parquet(dataPath);
        
        List<String> inputColumnNames = data.select("inputCols").head().getList(0);
        List<String> outputColumnNames = data.select("outputCols").head().getList(0);
        String handleInvalid = data.select("handleInvalid").head().getString(0);
        
        // Set the estimator
        MultiStringIndexer transformer = new MultiStringIndexer()
                .setInputCols(inputColumnNames)
                .setOutputCols(outputColumnNames);
        if (handleInvalid != null) {
            transformer.setHandleInvalid(handleInvalid);
        }
        
        DefaultParamsReader$.MODULE$.getAndSetParams(transformer, metadata, DefaultParamsReader$.MODULE$.getAndSetParams$default$3());
        //DefaultParamsReader$.MODULE$.getAndSetParams(transformer, metadata);
        return transformer;
    }
}
