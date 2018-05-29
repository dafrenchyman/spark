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

import com.mrsharky.spark.ml.feature.models.FloorCeilingModel;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.param.DoubleParam;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.util.DefaultParamsWritable;

/**
 *
 * @author Julien Pierret
 */
public class FloorCeiling extends Estimator<FloorCeilingModel> implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _inputCols;
    private DoubleParam _lowerPercentile;
    private DoubleParam _upperPercentile;
    private String _uid;
   
    @Override
    public String uid() {
        return _uid;
    }
    
    public FloorCeiling(String uid) {
        _uid = uid;
    }

    public FloorCeiling() {
        _uid = FloorCeiling.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    public String[] getInputCols() {
        return this.get(_inputCols).get();
    }
    
    public double getLowerPercentile() {
        return (double) this.get(_lowerPercentile).get();
    }
    
    public double getUpperPercentile() {
        return (double) this.get(_upperPercentile).get();
    }
    
    public FloorCeiling setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray(new String[columns.size()]);
        return setInputCols(columnsString);
    }
    
    public FloorCeiling setInputCols(String[] names) {
        _inputCols = inputCols();
        this.set(_inputCols, names);
        return this;
    }
    
    /**
     * Value must be between [0, 1]
     * @param value
     * @return 
     */
    public FloorCeiling setLowerPercentile(double value) {
        _lowerPercentile = lowerPercentile();
        this.set(_lowerPercentile, value);
        return this;
    }
    
    /**
     * Value must be between [0, 1]
     * @param value
     * @return 
     */
    public FloorCeiling setUpperPercentile(double value) {
        _upperPercentile = upperPercentile();
        this.set(_upperPercentile, value);
        return this;
    }
    
    @Override
    public StructType transformSchema(StructType oldSchema) {
        return oldSchema;
    }    
    
    @Override
    public FloorCeilingModel fit(Dataset<?> dataset) {
        String[] columns = this.getInputCols();
        double[] floors = new double[columns.length];
        double[] ceilings = new double[columns.length];
        double lowerPerc = this.getLowerPercentile()*100;
        double upperPerc = this.getUpperPercentile()*100;
        lowerPerc = lowerPerc <= 0  ? 0.01 : lowerPerc;
        upperPerc = upperPerc > 100 ? 100  : upperPerc;
        double[] min = new double[columns.length];
        double[] max = new double[columns.length];
        
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            List<Row> values = dataset.select(column).collectAsList();
            double[] valuesDouble = new double[values.size()];
            double currMin = Double.MAX_VALUE;
            double currMax = Double.MIN_VALUE;
            for (int j = 0; j < valuesDouble.length; j++) {
                Object value = values.get(j).get(0);
                if (value != null) {
                    if (value.getClass().equals(Long.class)) {
                        valuesDouble[j] = values.get(j).getLong(0);
                    } else if (value.getClass().equals(Double.class)) {
                        valuesDouble[j] = values.get(j).getDouble(0);
                    } else if (value.getClass().equals(Integer.class)) {
                        valuesDouble[j] = values.get(j).getInt(0);
                    } else if (value.getClass().equals(Float.class)) {
                        valuesDouble[j] = values.get(j).getFloat(0);
                    } else if (value.getClass().equals(Short.class)) {
                        valuesDouble[j] = values.get(j).getShort(0);
                    }
                    
                    // Min
                    if (valuesDouble[j] < currMin) {
                        currMin = valuesDouble[j];
                    }
                    
                    // Max
                    if (valuesDouble[j] > currMax) {
                        currMax = valuesDouble[j];
                    }
                }
            }
            
            Percentile perc = new Percentile();
            perc.setData(valuesDouble);
            floors[i] = perc.evaluate(lowerPerc);
            ceilings[i] = perc.evaluate(upperPerc);
            if (floors[i] == ceilings[i]) {
                floors[i] = currMin;
                ceilings[i] = currMax;
            }
            min[i] = currMin;
            max[i] = currMax;
        }
        
        FloorCeilingModel fcm = new FloorCeilingModel()
                .setInputCols(columns)
                .setCeils(ceilings)
                .setFloors(floors);
        return fcm;
    }

    @Override
    public Estimator<FloorCeilingModel> copy(ParamMap arg0) {
        FloorCeiling copied = new FloorCeiling()
                .setInputCols(this.getInputCols())
                .setLowerPercentile(this.getLowerPercentile())
                .setUpperPercentile(this.getUpperPercentile());
        return copied;
    }

    @Override
    public MLWriter write() {
        return new FloorCeilingWriter(this);
    }

    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }
    
    public static MLReader<FloorCeiling> read() {
        return new FloorCeilingReader();
    }
    
    public FloorCeiling load(String path) {
        return ((FloorCeilingReader)read()).load(path);
    }
    
    public void com$mrsharky$spark$ml$feature$FloorCeiling$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }
    
    public void com$mrsharky$spark$ml$feature$FloorCeiling$_setter_$lowerPercentiles_$eq(DoubleParam doubleParam) {
        this._lowerPercentile = doubleParam;
    }
    
    public void com$mrsharky$spark$ml$feature$FloorCeiling$_setter_$upperPercentiles_$eq(DoubleParam doubleParam) {
        this._upperPercentile = doubleParam;
    }
    
    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Name of columns to be processed");
    }
    
    public DoubleParam lowerPercentile() {
        return new DoubleParam(this, "lowerPercentile", "Lower percentile cutoff");
    }
    
    public DoubleParam upperPercentile() {     
        return new DoubleParam(this, "upperPercentile", "Upper percentile cutoff");
    }
}
