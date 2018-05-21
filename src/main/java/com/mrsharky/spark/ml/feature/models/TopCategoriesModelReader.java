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
public class TopCategoriesModelReader extends MLReader<TopCategoriesModel> implements Serializable {
    
    private String className = TopCategoriesModel.class.getName();
    
    public String getClassName() {
        return className;
    }
    
    public void setClassName(String className) {
        this.className = className;
    }
    
    @Override
    public TopCategoriesModel load(String path) {
        DefaultParamsReader.Metadata metadata = DefaultParamsReader$.MODULE$.loadMetadata(path, sc(), className);
        String dataPath = new Path(path, "data").toString();
        
        // Keys
        Row keysRow = sparkSession().read().parquet(dataPath).select("keys").head();
        List<String> keysList = keysRow.getList(0);
        String[] keys = keysList.toArray(new String[keysList.size()]);
        
        // Values
        Row valuesRow = sparkSession().read().parquet(dataPath).select("values").head();
        List<String> valuesList = valuesRow.getList(0);
        String[] values = valuesList.toArray(new String[valuesList.size()]);
        
        TopCategoriesModel transformer = new TopCategoriesModel()
                .setLookupMap(TopCategoriesHelpers.convertMapFromKeys(keys, values));
                
        DefaultParamsReader$.MODULE$.getAndSetParams(transformer, metadata);
        return transformer;
    }
}
