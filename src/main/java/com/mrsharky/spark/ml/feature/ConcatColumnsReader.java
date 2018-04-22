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
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ml.util.DefaultParamsReader;
import org.apache.spark.ml.util.DefaultParamsReader$;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.sql.Row;

/**
 *
 * @author Julien Pierret
 */
public class ConcatColumnsReader extends MLReader<ConcatColumns> implements Serializable {
    
    private String className = ConcatColumns.class.getName();
    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }
    
    @Override
    public ConcatColumns load(String path) {
        DefaultParamsReader.Metadata metadata = DefaultParamsReader$.MODULE$.loadMetadata(path, sc(), className);
        String dataPath = new Path(path, "data").toString();
        Row input = sparkSession().read().parquet(dataPath).select("inputCols").head();
        Row output = sparkSession().read().parquet(dataPath).select("outputCol").head();
        Row concatValue = sparkSession().read().parquet(dataPath).select("concatValue").head();
        List<String> listInputColumns = input.getList(0);
        String[] inputColumns = listInputColumns.toArray(new String[listInputColumns.size()]);
        ConcatColumns transformer = new ConcatColumns()
                .setInputCols(inputColumns)
                .setOutputCol(output.getString(0))
                .setConcatValue(concatValue.getString(0));
        DefaultParamsReader$.MODULE$.getAndSetParams(transformer, metadata);
        return transformer;
    }
}
