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

import static com.mrsharky.spark.utilities.DatasetUtils.ColumnExists;
import com.mrsharky.utilites.VersionHelper;
import java.io.Serializable;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
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
public class FloorCeilingMissingModelReader extends MLReader<FloorCeilingMissingModel> implements Serializable {
    
    private String className = FloorCeilingMissingModel.class.getName();
    
    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }
    
    @Override
    public FloorCeilingMissingModel load(String path) {
        DefaultParamsReader.Metadata metadata = DefaultParamsReader$.MODULE$.loadMetadata(path, sc(), className);
        String dataPath = new Path(path, "data").toString();
        Dataset<Row> data = sparkSession().read().parquet(dataPath);
        
        List<String> columnNames = data.select("inputCols").head().getList(0);
        
        FloorCeilingMissingModel transformer = new FloorCeilingMissingModel()
                .setInputCols(columnNames);
        
        if (!data.select("floors").head().isNullAt(0)) {
            List<Double> floorsList = data.select("floors").head().getList(0);
            Double[] floors = floorsList.toArray(new Double[floorsList.size()]);
            transformer = transformer.setFloors(floors);
        }
        
        if (!data.select("ceilings").head().isNullAt(0)) {
            List<Double> ceilsList = data.select("ceilings").head().getList(0);
            Double[] ceils = ceilsList.toArray(new Double[ceilsList.size()]);
            transformer = transformer.setCeilings(ceils);
        }
        
        if (!data.select("missings").head().isNullAt(0)) {
            List<Double> missList = data.select("missings").head().getList(0);
            Double[] missings = missList.toArray(new Double[missList.size()]);
            transformer = transformer.setMissings(missings);
        }

        //String version = data.sparkSession().version();
        DefaultParamsReader$.MODULE$.getAndSetParams(transformer, metadata, DefaultParamsReader$.MODULE$.getAndSetParams$default$3());
        //DefaultParamsReader$.MODULE$.getAndSetParams(transformer, metadata);

        return transformer;
    }
}
