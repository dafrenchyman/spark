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
public class FloorCeilingMissingWriter extends MLWriter implements Serializable {

    private FloorCeilingMissing instance;
    
    public FloorCeilingMissingWriter(FloorCeilingMissing instance) { this.instance = instance; }
    
    public FloorCeilingMissing getInstance() { return instance; } 
    public void setInstance(FloorCeilingMissing instance) { this.instance = instance; }
      
    @Override
    public void saveImpl(String path) {
        DefaultParamsWriter$.MODULE$.saveMetadata(instance, path, sc(), DefaultParamsWriter$.MODULE$
                .getMetadataToSave$default$3(), DefaultParamsWriter$.MODULE$.getMetadataToSave$default$4());
        Data data = new Data();
        data.setInputCols(instance.getInputCols());
        
        if (instance.hasLowerPercentile()) {
            data.setLowerPercentile(instance.getLowerPercentile());
        }
        
        if (instance.hasUpperPercentile()) {
            data.setUpperPercentile(instance.getUpperPercentile());
        }
        
        if (instance.hasMissingPercentile()) {
            data.setMissingPercentile(instance.getMissingPercentile());
        }
        
        List<Data> listData = new ArrayList<>();
        listData.add(data);
        String dataPath = new Path(path, "data").toString();
        sparkSession().createDataFrame(listData, Data.class).repartition(1).write().parquet(dataPath);
    }

    public static class Data implements Serializable {
        String[] _inputCols;
        Double _lowerPercentile;
        Double _upperPercentile;
        Double _missingPercentile;

        // Getters
        public String[] getInputCols()       { return _inputCols;         }
        public Double getLowerPercentile()   { return _lowerPercentile;   }
        public Double getUpperPercentile()   { return _upperPercentile;   }
        public Double getMissingPercentile() { return _missingPercentile; }
        
        // Setters
        public void setInputCols(String[] inputCols)               { _inputCols = inputCols;                 }
        public void setLowerPercentile(Double lowerPercentile)     { _lowerPercentile = lowerPercentile;     }
        public void setUpperPercentile(Double upperPercentile)     { _upperPercentile = upperPercentile;     }
        public void setMissingPercentile(Double missingPercentile) { _missingPercentile = missingPercentile; }
        
    }
}