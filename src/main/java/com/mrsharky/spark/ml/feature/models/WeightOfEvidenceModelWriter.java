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
package com.mrsharky.spark.ml.feature.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.ml.util.DefaultParamsWriter$;

/**
 *
 * @author Julien Pierret
 */
public class WeightOfEvidenceModelWriter extends MLWriter implements Serializable {

    private WeightOfEvidenceModel instance;
    
    public WeightOfEvidenceModelWriter(WeightOfEvidenceModel instance) {
        this.instance = instance;
    }
      
    public WeightOfEvidenceModel getInstance() {
        return instance;
    }
      
    public void setInstance(WeightOfEvidenceModel instance) {
        this.instance = instance;
    }
      
    @Override
    public void saveImpl(String path) {
        DefaultParamsWriter$.MODULE$.saveMetadata(instance, path, sc(), DefaultParamsWriter$.MODULE$
                .getMetadataToSave$default$3(), DefaultParamsWriter$.MODULE$.getMetadataToSave$default$4());
        Data data = new Data();
        
        data.setLookup(instance.getLookup());
        data.setInputCols(instance.getInputCols());
        data.setOutputCols(instance.getOutputCols());

        List<Data> listData = new ArrayList<>();
        listData.add(data);
        String dataPath = new Path(path, "data").toString();
        sparkSession().createDataFrame(listData, Data.class).repartition(1).write().parquet(dataPath);
    }

    public static class Data implements Serializable {
        private String[] _inputCols;
        private String[] _outputCols;
        private Map<String, Map<String, Double>> _lookup;

        // Getters
        public String[] getInputCols()                      { return _inputCols;  }
        public String[] getOutputCols()                     { return _outputCols; }
        public Map<String, Map<String, Double>> getLookup() { return _lookup;     }
        
        // Setters
        public void setInputCols(String[] inputCols)                   { _inputCols = inputCols;   }
        public void setOutputCols(String[] outputCols)                 { _outputCols = outputCols; }
        public void setLookup(Map<String, Map<String, Double>> lookup) { this._lookup = lookup;    }
    }
}