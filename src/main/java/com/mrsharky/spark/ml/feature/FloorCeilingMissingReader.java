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

import static com.mrsharky.spark.utilities.DatasetUtils.ColumnExists;
import java.io.Serializable;
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
public class FloorCeilingMissingReader extends MLReader<FloorCeilingMissing> implements Serializable {
    
    private String className = FloorCeilingMissing.class.getName();
    
    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }
    
    @Override
    public FloorCeilingMissing load(String path) {
        DefaultParamsReader.Metadata metadata = DefaultParamsReader$.MODULE$.loadMetadata(path, sc(), className);
        String dataPath = new Path(path, "data").toString();
        Dataset<Row> data = sparkSession().read().parquet(dataPath);
        
        List<String> columnNames = data.select("inputCols").head().getList(0);
        
        FloorCeilingMissing transformer = new FloorCeilingMissing()
                .setInputCols(columnNames);
        
        if (!data.select("lowerPercentile").head().isNullAt(0)) {
            double lowerPercentile = data.select("lowerPercentile").head().getDouble(0);
            transformer = transformer.setLowerPercentile(lowerPercentile);
        }
        
        if (!data.select("upperPercentile").head().isNullAt(0)) {
            double upperPercentile = data.select("upperPercentile").head().getDouble(0);
            transformer = transformer.setUpperPercentile(upperPercentile);
        }
        
        if (!data.select("missingPercentile").head().isNullAt(0)) {
            double missingPercentile = data.select("missingPercentile").head().getDouble(0);
            transformer = transformer.setMissingPercentile(missingPercentile);
        }
        
        DefaultParamsReader$.MODULE$.getAndSetParams(transformer, metadata, DefaultParamsReader$.MODULE$.getAndSetParams$default$3());
        //DefaultParamsReader$.MODULE$.getAndSetParams(transformer, metadata);
        return transformer;
    }
}
