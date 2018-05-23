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

import com.mrsharky.spark.ml.param.MapParam;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.util.DefaultParamsWritable;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

/**
 *
 * @author Julien Pierret
 */
public class TopCategoriesModel extends Model<TopCategoriesModel> implements Serializable, DefaultParamsWritable {
    
    private MapParam _lookup;
    private String _uid;
   
    public TopCategoriesModel(String uid) {
        _uid = uid;
    }

    public TopCategoriesModel() {
        _uid = TopCategoriesModel.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    public TopCategoriesModel setLookupMap(Map<String, List<String>> lookup) {
        this._lookup = lookup();
        this.set(this._lookup, convertFromListToObject(lookup));
        return this;
    }
    
    public Map<String, List<String>> getLookupMap() {
        return convertFromObjectToList(this.get(this._lookup).get());
    }
     
    @Override
    public Dataset<Row> transform(Dataset<?> data) {
        Dataset<Row> newData = null;
        StructType structType = data.schema();
        
        Map<String, List<Object>> columnsoProcess = convertToObject(this.getLookupMap());
        try {
            TopCategoriesFlatMap evfm = new TopCategoriesFlatMap(columnsoProcess, structType);
            ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
            newData = data.mapPartitions(f -> evfm.call((Iterator<Row>) f), encoder);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return newData;
    }

    @Override
    public StructType transformSchema(StructType oldSchema) {
        return oldSchema;
    }

    @Override
    public String uid() {
        return _uid;
    }
    
    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }   
    
    public void com$mrsharky$spark$ml$feature$models$TopCategoriesModel$_setter_$lookup_$eq(MapParam param) {
        this._lookup = param;
    }
 
    public MapParam lookup() {
        return new MapParam(this, "lookup", "Lookup Map<Key (Column), List<String> (categories)>");
    }
    
    @Override
    public TopCategoriesModel copy(ParamMap arg0) {
        TopCategoriesModel copied = new TopCategoriesModel()
                .setLookupMap(this.getLookupMap());
        return copied;
    }
    
    public static MLReader<TopCategoriesModel> read() {
        return new TopCategoriesModelReader();
    }
    
    public TopCategoriesModel load(String path) {
        return ((TopCategoriesModelReader)read()).load(path);
    }

    @Override
    public MLWriter write() {
        return new TopCategoriesModelWriter(this);
    }
    
    // Helpers
    public static Map<String, List<Object>> convertToObject(Map<String, List<String>> input) {
        Map<String, List<Object>> output = new HashMap<String, List<Object>>();
        for (String key : input.keySet()) {
            output.put(key, new ArrayList<Object>());
            for (String value : input.get(key)) {
                output.get(key).add(value);
            }
        }
        return output;
    }
    
    public static Map<String, List<String>> convertFromObjectToList(Map<String, Object> input) {
        Map<String, List<String>> output = new HashMap<String, List<String>>();
        for (String key : input.keySet()) {
            List<String> newInput = (List<String>) input.get(key);
            output.put(key, newInput);
        }
        return output;
    }
    
    public static Map<String, Object> convertFromListToObject(Map<String, List<String>> input) {
        Map<String, Object> output = new HashMap<String, Object>();
        for (String key : input.keySet()) {
            Object newInput = (Object) input.get(key);
            output.put(key, newInput);
        }
        return output;
    }
    
}
