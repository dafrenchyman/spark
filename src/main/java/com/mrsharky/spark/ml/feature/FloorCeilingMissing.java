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

import com.mrsharky.spark.ml.feature.models.FloorCeilingMissingModel;
import com.mrsharky.spark.ml.param.DoubleParam2;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.param.DoubleParam;
import org.apache.spark.ml.param.Param;
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
public class FloorCeilingMissing extends Estimator<FloorCeilingMissingModel> implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _inputCols;
    private DoubleParam _lowerPercentile;
    private DoubleParam _upperPercentile;
    private DoubleParam _missingPercentile;
    private String _uid;
   
    @Override
    public String uid() {
        return _uid;
    }
    
    public FloorCeilingMissing(String uid) {
        _uid = uid;
        /*_lowerPercentile = lowerPercentile();
        this.set(_lowerPercentile, null);
        _upperPercentile = upperPercentile();
        this.set(_upperPercentile, null);
        _missingPercentile = missingPercentile();
        this.set(_missingPercentile, null);*/
    }

    public FloorCeilingMissing() {
        _uid = FloorCeilingMissing.class.getName() + "_" + UUID.randomUUID().toString();
        /*_lowerPercentile = lowerPercentile();
        this.set(_lowerPercentile, null);
        _upperPercentile = upperPercentile();
        this.set(_upperPercentile, null);
        _missingPercentile = missingPercentile();
        this.set(_missingPercentile, null);*/
    }
    
    public String[] getInputCols() {
        return this.get(_inputCols).get();
    }
    
    public Double getLowerPercentile() {
        return (Double) this.get(_lowerPercentile).get();
    }
    
    public Double getUpperPercentile() {
        return (Double) this.get(_upperPercentile).get();
    }
    
    public Double getMissingPercentile() {
        return (Double) this.get(_missingPercentile).get();
    }
    
    public FloorCeilingMissing setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray(new String[columns.size()]);
        return setInputCols(columnsString);
    }
    
    public FloorCeilingMissing setInputCols(String[] names) {
        _inputCols = inputCols();
        this.set(_inputCols, names);
        return this;
    }
    
    /**
     * Value must be between [0, 1]
     * @param value
     * @return 
     */
    public FloorCeilingMissing setLowerPercentile(double value) {
        _lowerPercentile = lowerPercentile();
        this.set(_lowerPercentile, value);
        return this;
    }
    
    /**
     * Value must be between [0, 1]
     * @param value
     * @return 
     */
    public FloorCeilingMissing setUpperPercentile(double value) {
        _upperPercentile = upperPercentile();
        this.set(_upperPercentile, value);
        return this;
    }
    
    public FloorCeilingMissing setMissingPercentile(double value) {
        _missingPercentile = missingPercentile();
        this.set(_missingPercentile, value);
        return this;
    }
    
    public boolean hasLowerPercentile() {
        return !this.get(this.lowerPercentile()).isEmpty();
    }
    
    public boolean hasUpperPercentile() {
        return !this.get(this.upperPercentile()).isEmpty();
    }
    
    public boolean hasMissingPercentile() {
        return !this.get(this.missingPercentile()).isEmpty();
    }
    
    @Override
    public StructType transformSchema(StructType oldSchema) {
        return oldSchema;
    }    
    
    @Override
    public FloorCeilingMissingModel fit(Dataset<?> dataset) {
        String[] columns = this.getInputCols();
        Double[] floors = new Double[columns.length];
        Double[] ceilings = new Double[columns.length];
        Double[] missings = new Double[columns.length];
        
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
            
            // Lower Percentile
            if (this.hasLowerPercentile()) {
                double lowerPerc = this.getLowerPercentile()*100;
                lowerPerc = lowerPerc <= 0  ? 0.0 : lowerPerc;
                if (lowerPerc == 0.0) {
                    floors[i] = currMin;
                } else {
                    floors[i] = perc.evaluate(lowerPerc);
                }
            }
            
            // Upper Percentile
            if (this.hasUpperPercentile()) {
                double upperPerc = this.getUpperPercentile()*100;
                upperPerc = upperPerc > 100 ? 100 : upperPerc;
                if (upperPerc == 100.0) {
                    ceilings[i] = currMax;
                } else {
                    ceilings[i] = perc.evaluate(upperPerc);
                }
            }
            
            // Missing Percentile
            if (this.hasMissingPercentile()) {
                double missingPerc = this.getMissingPercentile()*100;
                missingPerc = missingPerc <= 0  ? 0.0 : missingPerc;
                missingPerc = missingPerc > 100 ? 100 : missingPerc;
                if (missingPerc == 100.0) {
                    missings[i] = currMax;
                } else if (missingPerc == 0.0) {
                    missings[i] = currMin;
                } else {
                    missings[i] = perc.evaluate(missingPerc);
                }
            }
            
            // Sometimes floor/ceiling can be the same value (for booleans) this forces it so it won't be
            // TODO: Should make this optional
            if (!this.get(this.lowerPercentile()).isEmpty() && !this.get(this.upperPercentile()).isEmpty()) {
                if (floors[i] == ceilings[i]) {
                    floors[i] = currMin;
                    ceilings[i] = currMax;
                }
            }
        }
        
        // Generate the model now that we have everything
        FloorCeilingMissingModel fcm = new FloorCeilingMissingModel()
                .setInputCols(columns);
        if (!this.get(this.upperPercentile()).isEmpty())  {
            fcm = fcm.setCeilings(ceilings);
        }
        
        if (!this.get(this.lowerPercentile()).isEmpty()) {
            fcm = fcm.setFloors(floors);
        }
        
        return fcm;
    }

    @Override
    public Estimator<FloorCeilingMissingModel> copy(ParamMap arg0) {
        FloorCeilingMissing copied = new FloorCeilingMissing()
                .setInputCols(this.getInputCols());
        
        if (!this.get(this.missingPercentile()).isEmpty()) {
            copied = copied.setMissingPercentile(this.getMissingPercentile());
        }
        
        if (!this.get(this.lowerPercentile()).isEmpty()) {
            copied = copied.setLowerPercentile(this.getLowerPercentile());
        }
        
        if (!this.get(this.upperPercentile()).isEmpty()) {
            copied = copied.setUpperPercentile(this.getUpperPercentile());
        }
                
        return copied;
    }

    @Override
    public MLWriter write() {
        return new FloorCeilingMissingWriter(this);
    }

    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }
    
    public static MLReader<FloorCeilingMissing> read() {
        return new FloorCeilingMissingReader();
    }
    
    public FloorCeilingMissing load(String path) {
        return ((FloorCeilingMissingReader)read()).load(path);
    }
    
    public void com$mrsharky$spark$ml$feature$FloorCeiling$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }
    
    public void com$mrsharky$spark$ml$feature$FloorCeiling$_setter_$lowerPercentile_$eq(DoubleParam doubleParam) {
        this._lowerPercentile = doubleParam;
    }
    
    public void com$mrsharky$spark$ml$feature$FloorCeiling$_setter_$upperPercentile_$eq(DoubleParam doubleParam) {
        this._upperPercentile = doubleParam;
    }
    
    public void com$mrsharky$spark$ml$feature$FloorCeiling$_setter_$missingPercentile_$eq(DoubleParam doubleParam) {
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
    
    public DoubleParam missingPercentile() {     
        return new DoubleParam(this, "missingPercentile", "Percentile to use for missing values");
    }
}
