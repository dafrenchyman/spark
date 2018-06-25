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
import org.javatuples.Triplet;

/**
 *
 * @author Julien Pierret
 */
public class FloorCeilingMissingModel extends Model<FloorCeilingMissingModel> implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _inputCols;
    private DoubleArrayParam _floors;
    private DoubleArrayParam _ceilings;
    private DoubleArrayParam _missings;
    private String _uid;
   
    public FloorCeilingMissingModel(String uid) {
        _uid = uid;
    }

    public FloorCeilingMissingModel() {
        _uid = FloorCeilingMissingModel.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    public FloorCeilingMissingModel setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray(new String[columns.size()]);
        return setInputCols(columnsString);
    }
    
    public FloorCeilingMissingModel setInputCols(String[] names) {
        _inputCols = inputCols();
        this.set(_inputCols, names);
        return this;
    }
    
    public FloorCeilingMissingModel setFloors(double[] floors) {
        this._floors = floors();
        this.set(this._floors, floors);
        return this;
    }
    
    public FloorCeilingMissingModel setCeilings(double[] ceils) {
        this._ceilings = ceilings();
        this.set(this._ceilings, ceils);
        return this;
    }
    
    public FloorCeilingMissingModel setMissings(double[] ceils) {
        this._missings = ceilings();
        this.set(this._missings, ceils);
        return this;
    }
    
    public String[] getInputCols() {
        return this.get(_inputCols).get();
    }
    
    public double[] getFloors() {
        return this.get(_floors).get();
    }
    
    public double[] getCeilings() {
        return this.get(_ceilings).get();
    }
    
    public double[] getMissings() {
        return this.get(_missings).get();
    }
    
    public boolean hasFloors() {
        return !this.get(this.floors()).isEmpty();
    }
    
    public boolean hasCeilings() {
        return !this.get(this.ceilings()).isEmpty();
    }
    
    public boolean hasMissings() {
        return !this.get(this.missings()).isEmpty();
    }
     
    private Map<String, Triplet<Double, Double, Double>> generateColumnsToProcess() {
        String[] columnNames  = this.get(_inputCols).get();
        double[] columnFloors = hasFloors()   ? this.get(_floors).get()    : null;
        double[] columnCeils  = hasCeilings() ? this.get(_ceilings ).get() : null;
        double[] columnMiss   = hasMissings() ? this.get(_missings ).get() : null;
        Map<String, Triplet<Double, Double, Double>> columnsToProcess = new HashMap<String, Triplet<Double, Double, Double>>();
        for (int i = 0; i < columnNames.length; i++) {
            String currName  = columnNames[i];
            Double currFloor = columnFloors != null ? columnFloors[i] : null;
            Double currCeil  = columnCeils  != null ? columnCeils[i]  : null;
            Double currMiss  = columnMiss   != null ? columnMiss[i]   : null;
            columnsToProcess.put(currName, Triplet.with(currFloor, currCeil, currMiss));
        }
        return columnsToProcess;
    }
    
    @Override
    public Dataset<Row> transform(Dataset<?> data) {
        Dataset<Row> newData = null;
        StructType structType = data.schema();     
        Map<String, Triplet<Double, Double, Double>> columnsToFloorCeiling = generateColumnsToProcess(); 
        try {
            FloorCeilingMissingFlatMap evfm = new FloorCeilingMissingFlatMap(columnsToFloorCeiling, structType);
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

    public void com$mrsharky$spark$ml$feature$models$FloorCeilingModel$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }
    
    public void com$mrsharky$spark$ml$feature$models$FloorCeilingModel$_setter_$floors_$eq(DoubleArrayParam doubleArrayParam) {
        this._floors = doubleArrayParam;
    }
    
    public void com$mrsharky$spark$ml$feature$models$FloorCeilingModel$_setter_$ceilings_$eq(DoubleArrayParam doubleArrayParam) {
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
    
    public DoubleArrayParam missings() {
        return new DoubleArrayParam(this, "missings", "Missing values");
    }
 
    @Override
    public FloorCeilingMissingModel copy(ParamMap arg0) {
        FloorCeilingMissingModel copied = new FloorCeilingMissingModel()
                .setInputCols(this.getInputCols());
        
        if (!this.get(this.missings()).isEmpty()) {
            copied = copied.setMissings(this.getMissings());
        }
        
        if (!this.get(this.floors()).isEmpty()) {
            copied = copied.setMissings(this.getFloors());
        }
        
        if (!this.get(this.ceilings()).isEmpty()) {
            copied = copied.setMissings(this.getCeilings());
        }

        return copied;
    }
    
    public static MLReader<FloorCeilingMissingModel> read() {
        return new FloorCeilingMissingModelReader();
    }
    
    public FloorCeilingMissingModel load(String path) {
        return ((FloorCeilingMissingModelReader)read()).load(path);
    }

    @Override
    public MLWriter write() {
        return new FloorCeilingMissingModelWriter(this);
    }

}
