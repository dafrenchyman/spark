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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ml.util.DefaultParamsReader;
import org.apache.spark.ml.util.DefaultParamsReader$;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.javatuples.Pair;

/**
 *
 * @author Julien Pierret
 */
public class ExplodeVectorReader extends MLReader<ExplodeVector> implements Serializable {
    
    private String className = ExplodeVector.class.getName();
    
    public String getClassName() { return className; }   
    public void setClassName(String className) { className = className; }
    
    @Override
    public ExplodeVector load(String path) {
        DefaultParamsReader.Metadata metadata = DefaultParamsReader$.MODULE$.loadMetadata(path, sc(), className);
        String dataPath = new Path(path, "data").toString();
        Dataset<Row> data = sparkSession().read().parquet(dataPath);
        
        List<String> columnNames = data.select("inputCols").head().getList(0);
        List<Integer> columnSize = data.select("inputColsSize").head().getList(0);
        
        // Create a Pair object to submit to the class for construction
        List<Pair<String, Integer>> pairs = new ArrayList<Pair<String, Integer>>();
        for (int i = 0; i < columnNames.size(); i++) {
            pairs.add(Pair.with(columnNames.get(i), columnSize.get(i)));
        }
        ExplodeVector transformer = new ExplodeVector()
                .setInputCols(pairs);
        DefaultParamsReader$.MODULE$.getAndSetParams(transformer, metadata);
        return transformer;
    }
}
