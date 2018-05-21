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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.param.DoubleArrayParam;
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
public class FloorCeilingModel extends Model<FloorCeilingModel> implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _inputCols;
    private DoubleArrayParam _floors;
    private DoubleArrayParam _ceilings;
    private String _uid;
   
    public FloorCeilingModel(String uid) {
        _uid = uid;
    }

    public FloorCeilingModel() {
        _uid = FloorCeilingModel.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    public FloorCeilingModel setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray(new String[columns.size()]);
        return setInputCols(columnsString);
    }
    
    public FloorCeilingModel setInputCols(String[] names) {
        _inputCols = inputCols();
        this.set(_inputCols, names);
        return this;
    }
    
    public FloorCeilingModel setFloors(double[] floors) {
        this._floors = floors();
        this.set(this._floors, floors);
        return this;
    }
    
    public FloorCeilingModel setCeils(double[] ceils) {
        this._ceilings = ceilings();
        this.set(this._ceilings, ceils);
        return this;
    }
    
    public String[] getInputCols() {
        return this.get(_inputCols).get();
    }
    
    public double[] getFloors() {
        return this.get(_floors).get();
    }
    
    public double[] getCeil() {
        return this.get(_ceilings).get();
    }
     
    private Map<String, Pair<Double, Double>> generateColumnsToProcess() {
        String[] columnNames  = this.get(_inputCols).get();
        double[] columnFloors = this.get(_floors).get();
        double[] columnCeils  = this.get(_ceilings ).get();
        Map<String, Pair<Double, Double>> columnsToProcess = new HashMap<String, Pair<Double, Double>>();
        for (int i = 0; i < columnNames.length; i++) {
            String currName  = columnNames[i];
            double currFloor = columnFloors[i];
            double currCeil  = columnCeils[i];
            columnsToProcess.put(currName, Pair.with(currFloor, currCeil));
        }
        return columnsToProcess;
    }
    
    @Override
    public Dataset<Row> transform(Dataset<?> data) {
        Dataset<Row> newData = null;
        StructType structType = data.schema();     
        Map<String, Pair<Double, Double>> columnsToFloorCeiling = generateColumnsToProcess(); 
        try {
            FloorCeilingFlatMap evfm = new FloorCeilingFlatMap(columnsToFloorCeiling, structType);
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

    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }
    
    public void org$apache$spark$ml$param$shared$HasFloors$_setter_$floors_$eq(DoubleArrayParam doubleArrayParam) {
        this._floors = doubleArrayParam;
    }
    
    public void org$apache$spark$ml$param$shared$HasCeilings$_setter_$ceilings_$eq(DoubleArrayParam doubleArrayParam) {
        this._ceilings = doubleArrayParam;
    }
    
    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Name of columns to be processed");
    }
    
    public DoubleArrayParam floors() {
        return new DoubleArrayParam(this, "floors", "Floor values");
    }

    public DoubleArrayParam ceilings() {
        return new DoubleArrayParam(this, "ceilings", "Ceiling values");
    }
 
    @Override
    public FloorCeilingModel copy(ParamMap arg0) {
        FloorCeilingModel copied = new FloorCeilingModel()
                .setInputCols(this.getInputCols())
                .setFloors(this.getFloors())
                .setCeils(this.getCeil());
        return copied;
    }
    
    public static MLReader<FloorCeilingModel> read() {
        return new FloorCeilingModelReader();
    }
    
    public FloorCeilingModel load(String path) {
        return ((FloorCeilingModelReader)read()).load(path);
    }

    @Override
    public MLWriter write() {
        return new FloorCeilingModelWriter(this);
    }

}
