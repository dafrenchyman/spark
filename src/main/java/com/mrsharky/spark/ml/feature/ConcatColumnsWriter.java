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
public class ConcatColumnsWriter extends MLWriter implements Serializable {

    private ConcatColumns _instance;
    
    public ConcatColumnsWriter(ConcatColumns instance) { _instance = instance; } 
    public ConcatColumns getInstance() { return _instance; }
    public void setInstance(ConcatColumns instance) { _instance = instance; }
      
    @Override
    public void saveImpl(String path) {
        DefaultParamsWriter$.MODULE$.saveMetadata(_instance, path, sc(), DefaultParamsWriter$.MODULE$
                .getMetadataToSave$default$3(), DefaultParamsWriter$.MODULE$.getMetadataToSave$default$4());
        Data data = new Data();
        data.setInputCols(_instance.getInputCols());
        data.setOutputCol(_instance.getOutputCol());
        data.setConcatValue(_instance.getConcatValue());
        List<Data> listData = new ArrayList<>();
        listData.add(data);
        String dataPath = new Path(path, "data").toString();
        sparkSession().createDataFrame(listData, Data.class).repartition(1).write().parquet(dataPath);
    }

    public static class Data implements Serializable {
        private String[] _inputCols;
        private String _outputCol;
        private String _concatValue;

        // Getters
        public String[] getInputCols()  { return _inputCols; }
        public String getOutputCol()    { return _outputCol; }
        public String getConcatValue() { return _concatValue; }
        
        // Setters
        public void setInputCols(String[] inputCols) { _inputCols = inputCols; }
        public void setOutputCol(String outputCol)   { _outputCol = outputCol; }
        public void setConcatValue(String concatValue)    { _concatValue = concatValue; }
    }
}