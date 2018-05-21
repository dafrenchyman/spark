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
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.util.DefaultParamsWritable;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.javatuples.Pair;

/**
 *
 * @author Julien Pierret
 */
public class TopCategoriesModel extends Model<TopCategoriesModel> implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _keys;
    private StringArrayParam _values;
    private String _uid;
   
    public TopCategoriesModel(String uid) {
        _uid = uid;
    }

    public TopCategoriesModel() {
        _uid = TopCategoriesModel.class.getName() + "_" + UUID.randomUUID().toString();
    }
     
    public TopCategoriesModel setLookupMap(Map<String, List<String>> keys) {
        this._keys = inputKeys();
        this._values = inputValues();
        Pair<String[], String[]> pairs = TopCategoriesHelpers.convertKeysFromMap(keys);
        this.set(this._keys, pairs.getValue0());
        this.set(this._values, pairs.getValue1());
        return this;
    }
     
    public Map<String, List<String>> getLookupMap() {
        String[] keys = this.get(this._keys).get();
        String[] values = this.get(this._values).get();
        return TopCategoriesHelpers.convertMapFromKeys(keys, values);
    }
     
    @Override
    public Dataset<Row> transform(Dataset<?> data) {
        Dataset<Row> newData = null;
        StructType structType = data.schema();     
        Map<String, List<Object>> columnsoProcess = TopCategoriesHelpers.convertToObject(getLookupMap());
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
    
    public void org$apache$spark$ml$param$shared$HasKey$_setter_$key_$eq(StringArrayParam stringArrayParam) {
        this._keys = stringArrayParam;
    }
    
    public void org$apache$spark$ml$param$shared$HasValues$_setter_$values_$eq(StringArrayParam stringArrayParam) {
        this._values = stringArrayParam;
    }

    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Name of columns to be processed");
    }
    
    public StringArrayParam inputKeys() {
        return new StringArrayParam(this, "inputKeys", "Key columns");
    }
    
    public StringArrayParam inputValues() {
        return new StringArrayParam(this, "inputValues", "Values for key columns");
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
}
