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
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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
import static org.apache.spark.sql.functions.col;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

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
    
    /**
     * https://stackoverflow.com/questions/28805602/how-to-compute-percentiles-in-apache-spark
     * @param rdd
     * @param percentiles
     * @param rddSize
     * @param numPartitions
     * @return 
     */
    private static Double[] getPercentiles(JavaRDD<Double> rdd, Double[] percentiles) {
        long rddSize = rdd.count();
        int numPartitions = rdd.getNumPartitions();
        Double[] values = new Double[percentiles.length];
        JavaRDD<Double> sorted = rdd.sortBy((Double d) -> d, true, numPartitions);
        JavaPairRDD<Long, Double> indexed = sorted.zipWithIndex().mapToPair((Tuple2<Double, Long> t) -> t.swap());

        for (int i = 0; i < percentiles.length; i++) {
            Double percentile = percentiles[i];
            if (percentile != null) {
                long id = (long) (rddSize * percentile);
                values[i] = indexed.lookup(id).get(0);
            } else {
                values[i] = null;
            }
        }
        return values;
    }
    
    private JavaRDD<Double> rowToDouble(Dataset<Row> dataset) {
        JavaRDD<Row> data = dataset.toJavaRDD();
        JavaRDD<Double> dataDouble = data.map(new Function<Row,Double>() {
            public Double call(Row s) {
                Object value = s.get(0);
                Double val = null; 
                if (value != null) {
                    if (value.getClass().equals(Double.class)) {
                        val = s.getDouble(0);
                    } else if (value.getClass().equals(Integer.class)) {
                        val = (double) s.getInt(0);
                    } else if (value.getClass().equals(Float.class)) {
                        val = (double) s.getFloat(0);
                    } else if (value.getClass().equals(Long.class)) {
                        val = (double) s.getLong(0);
                    } else if (value.getClass().equals(Short.class)) {
                        val = (double) s.getShort(0);
                    }
                }
                return val;
            }
        });
        return dataDouble;
    }
    
    @Override
    public FloorCeilingMissingModel fit(Dataset<?> dataset) {
        String[] columns  = this.getInputCols();
        Double[] floors   = new Double[columns.length];
        Double[] ceilings = new Double[columns.length];
        Double[] missings = new Double[columns.length];
        
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            Dataset subset = dataset.select(column).where(col(column).isNotNull());
            
            JavaRDD<Double> doubleData = rowToDouble(subset);
            doubleData.persist(StorageLevel.DISK_ONLY());
            
            Double[] percentiles = new Double[3];
            if (this.hasLowerPercentile()) {
                percentiles[0] = this.getLowerPercentile();
            } else {
                percentiles[0] = null;
            }
            
            if (this.hasUpperPercentile()) {
                percentiles[1] = this.getUpperPercentile();
            } else {
                percentiles[1] = null;
            }
            
            if (this.hasMissingPercentile()) {
                percentiles[2] = this.getMissingPercentile();
            } else {
                percentiles[2] = null;
            }
            
            Double[] values = getPercentiles(doubleData, percentiles);
            
            if (this.hasLowerPercentile()) {
                floors[i] = values[0];
            } else {
                floors[i] = null;
            }
            
            if (this.hasUpperPercentile()) {
                ceilings[i] = values[1];
            } else {
                ceilings[i] = null;
            }
            
            if (this.hasMissingPercentile()) {
                missings[i] = values[2];
            } else {
                missings[i] = null;
            }
        }
        
        // Generate the model now that we have everything
        FloorCeilingMissingModel fcm = new FloorCeilingMissingModel()
                .setInputCols(columns);
        if (this.hasLowerPercentile()) {
            fcm = fcm.setFloors(floors);
        }
        
        if (this.hasUpperPercentile()) {
            fcm = fcm.setCeilings(ceilings);
        }
        
        if (this.hasMissingPercentile()) {
            fcm = fcm.setMissings(missings);
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
