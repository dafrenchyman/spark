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
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.ml.util.DefaultParamsWriter$;

/**
 *
 * @author Julien Pierret
 */
public class DropColumnsWriter extends MLWriter implements Serializable {

    private DropColumns _instance;
    
    public DropColumnsWriter(DropColumns instance) { _instance = instance; }
    public DropColumns getInstance() { return _instance; }
    public void setInstance(DropColumns instance) { _instance = instance; }
      
    @Override
    public void saveImpl(String path) {
        DefaultParamsWriter$.MODULE$.saveMetadata(_instance, path, sc(), DefaultParamsWriter$.MODULE$
                .getMetadataToSave$default$3(), DefaultParamsWriter$.MODULE$.getMetadataToSave$default$4());
        Data data = new Data();
        data.setInputCols(_instance.getInputCols());
        List<Data> listData = new ArrayList<>();
        listData.add(data);
        String dataPath = new Path(path, "data").toString();
        sparkSession().createDataFrame(listData, Data.class).repartition(1).write().parquet(dataPath);
    }

    public static class Data implements Serializable {
        private String[] _inputCols;

        public String[] getInputCols() { return _inputCols; }
        public void setInputCols(String[] inputCols) { _inputCols = inputCols; }
    }
}