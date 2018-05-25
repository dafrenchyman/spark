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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 *
 * @author Julien Pierret
 */
public class WeightOfEvidenceModel extends Model<WeightOfEvidenceModel> implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _inputCols;
    private StringArrayParam _outputCols;
    private MapParam _lookup;
    private String _uid;
   
    public WeightOfEvidenceModel(String uid) {
        _uid = uid;
    }

    public WeightOfEvidenceModel() {
        _uid = WeightOfEvidenceModel.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    public WeightOfEvidenceModel setLookup(Map<String, Map<String, Double>> lookup) {
        this._lookup = lookup();
        this.set(_lookup, convertFromMapToObject(lookup));
        return this;
    }
    
    public WeightOfEvidenceModel setInputCols(String[] names) {
        String [] outputs = new String[names.length];
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = names[i] + "_woe";
        }
        _inputCols = inputCols();
        _outputCols = outputCols();
        this.set(_inputCols, names);
        this.set(_outputCols, outputs);
        return this;
    }
      
    public WeightOfEvidenceModel setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray(new String[columns.size()]);
        return setInputCols(columnsString);
    }
    
    public WeightOfEvidenceModel setInputCols(String name) {
        _inputCols = inputCols();
        String[] names = new String[]{name};
        return setInputCols(names);
    }
    
    public WeightOfEvidenceModel setOutputCols(String[] names) {
        _outputCols = outputCols();
        this.set(_outputCols, names);
        return this;
    }
    
    public WeightOfEvidenceModel setOutputCols(List<String> names) {
        String[] columnsString = names.toArray(new String[names.size()]);
        return setOutputCols(columnsString);
    }
    
    public String[] getInputCols() {
        return this.get(_inputCols).get();
    }
    
    public String[] getOutputCols() {
        return this.get(_outputCols).get();
    }
    
    public Map<String, Map<String, Double>> getLookup() {
        return convertFromObjectToMap(this.get(_lookup).get());
    }
    
    @Override
    public Dataset<Row> transform(Dataset<?> data) {
        Dataset<Row> newData = null;
        StructType structType = data.schema();
        Map<String, Map<String, Double>> lookup = this.getLookup();
        try {
            if (this.getInputCols().length != this.getInputCols().length) {
                throw new Exception("outputCols must be of same length as inputCols");
            }
            WeightOfEvidenceFlatMap evfm = new WeightOfEvidenceFlatMap(lookup, structType);
            ExpressionEncoder<Row> encoder = RowEncoder.apply(transformSchema(structType));
            newData = data.mapPartitions(f -> evfm.call((Iterator<Row>) f), encoder);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return newData;
    }

    @Override
    public StructType transformSchema(StructType oldSchema) {
        String[] inputCols  = this.getInputCols();
        String[] outputCols = this.getOutputCols();
        
        List<String> columnsToProcess = Arrays.asList(inputCols);
        List<String> outputsToProcess = Arrays.asList(outputCols);
        
        // Generate the Schema & find the columns to extract
        StructField[] currFields = oldSchema.fields(); 
        List<StructField> newFields = new ArrayList<StructField>();
        
        for (int i = 0; i < currFields.length; i++) {
            StructField currField = currFields[i];
            String fieldName = currField.name();
            DataType currDataType = currField.dataType();
            boolean currNullable = currField.nullable();
            newFields.add(DataTypes.createStructField(fieldName, currDataType, currNullable));
            if (columnsToProcess.contains(fieldName)) {
                int index = columnsToProcess.indexOf(fieldName);
                String outputColumnName = outputsToProcess.get(index);
                newFields.add(DataTypes.createStructField(outputColumnName, DataTypes.DoubleType, true));
            }
        }
        return DataTypes.createStructType(newFields);
    }

    @Override
    public String uid() {
        return _uid;
    }
    
    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }

    public void com$mrsharky$spark$ml$feature$models$WeightOfEvidenceModel$_setter_$lookup_$eq(MapParam stringArrayParam) {
        this._lookup = stringArrayParam;
    }
    
    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Name of columns to be processed");
    }
    
    public StringArrayParam outputCols() {
        return new StringArrayParam(this, "outputCols", "Name of columns for results");
    }
    
    public MapParam lookup() {
        return new MapParam(this, "lookup", "Lookup Map<Key (Column), Map<Object, Double> (category, woe)>");
    }
 
    @Override
    public WeightOfEvidenceModel copy(ParamMap arg0) {
        WeightOfEvidenceModel copied = new WeightOfEvidenceModel()
                .setInputCols(this.getInputCols())
                .setOutputCols(this.getOutputCols())
                .setLookup(this.getLookup());
        return copied;
    }
    
    public static MLReader<WeightOfEvidenceModel> read() {
        return new WeightOfEvidenceModelReader();
    }
    
    public WeightOfEvidenceModel load(String path) {
        return ((WeightOfEvidenceModelReader)read()).load(path);
    }

    @Override
    public MLWriter write() {
        return new WeightOfEvidenceModelWriter(this);
    }
    
    public static Map<String, Map<String, Double>> convertFromObjectToMap(Map<String, Object> input) {
        Map<String, Map<String, Double>> output = new HashMap<String, Map<String, Double>>();
        for (String key : input.keySet()) {
            Map<String, Double> newInput = (Map<String, Double>) input.get(key);
            output.put(key, newInput);
        }
        return output;
    }
    
    public static Map<String, Object> convertFromMapToObject(Map<String, Map<String, Double>> input) {
        Map<String, Object> output = new HashMap<String, Object>();
        for (String key : input.keySet()) {
            Object newInput = (Object) input.get(key);
            output.put(key, newInput);
        }
        return output;
    }
}
