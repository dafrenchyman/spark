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

import com.mrsharky.spark.ml.feature.TopCategoriesHelpers;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.ml.util.DefaultParamsWriter$;
import org.javatuples.Pair;

/**
 *
 * @author Julien Pierret
 */
public class TopCategoriesModelWriter extends MLWriter implements Serializable {

    private TopCategoriesModel instance;
    
    public TopCategoriesModelWriter(TopCategoriesModel instance) {
        this.instance = instance;
    }
      
    public TopCategoriesModel getInstance() {
        return instance;
    }
      
    public void setInstance(TopCategoriesModel instance) {
        this.instance = instance;
    }
      
    @Override
    public void saveImpl(String path) {
        DefaultParamsWriter$.MODULE$.saveMetadata(instance, path, sc(), DefaultParamsWriter$.MODULE$
                .getMetadataToSave$default$3(), DefaultParamsWriter$.MODULE$.getMetadataToSave$default$4());
        Data data = new Data();
        
        Map<String, List<String>> lookups = instance.getLookupMap();
        Pair<String[], String[]> pairs = TopCategoriesHelpers.convertKeysFromMap(lookups);   
        data.setKeys(pairs.getValue0());
        data.setValues(pairs.getValue1());
        List<Data> listData = new ArrayList<>();
        listData.add(data);
        String dataPath = new Path(path, "data").toString();
        sparkSession().createDataFrame(listData, Data.class).repartition(1).write().parquet(dataPath);
    }

    public static class Data implements Serializable {
        private String[] _keys;
        private String[] _values;

        // Getters
        public String[] getKeys()   { return _keys;   }
        public String[] getValues() { return _values; }
        
        // Setters
        public void setKeys(String[] keys)     { this._keys = keys;     }
        public void setValues(String[] values) { this._values = values; }
    }
}