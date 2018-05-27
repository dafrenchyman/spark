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

import com.mrsharky.spark.ml.feature.models.WeightOfEvidenceModel;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.IntParam;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.util.DefaultParamsWritable;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Julien Pierret
 */
public class WeightOfEvidence extends Estimator<WeightOfEvidenceModel> implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _inputCols;
    private StringArrayParam _outputCols;
    private Param<String> _booleanResponseCol;
    private IntParam _minObs;
    private String _uid;
   
    @Override
    public String uid() {
        return _uid;
    }
    
    public WeightOfEvidence(String uid) {
        _uid = uid;
    }

    public WeightOfEvidence() {
        _uid = WeightOfEvidence.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    public String[] getInputCols() {
        return this.get(_inputCols).get();
    }
    
    public String[] getOutputCols() {
        return this.get(_outputCols).get();
    }
    
    public String getBooleanResponseCol() {
        return this.get(this._booleanResponseCol).get();
    }
    
    public int getMinObs() {
        return (int) this.get(this._minObs).get();
    }
    
    public WeightOfEvidence setInputCols(String[] names) {
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
      
    public WeightOfEvidence setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray(new String[columns.size()]);
        return setInputCols(columnsString);
    }
    
    public WeightOfEvidence setInputCols(String name) {
        _inputCols = inputCols();
        String[] names = new String[]{name};
        return setInputCols(names);
    }
    
    public WeightOfEvidence setOutputCols(String[] names) {
        _outputCols = outputCols();
        this.set(_outputCols, names);
        return this;
    }
    
    public WeightOfEvidence setOutputCols(List<String> names) {
        String[] columnsString = names.toArray(new String[names.size()]);
        return setOutputCols(columnsString);
    }
    
    public WeightOfEvidence setBooleanResponseCol(String name) {
        _booleanResponseCol = booleanResponseCol();
        this.set(_booleanResponseCol, name);
        return this;
    }
    
    public WeightOfEvidence setMinObs(int minObs) {
        _minObs = minObs();
        this.set(_minObs, minObs);
        return this;
    }
    
    @Override
    public StructType transformSchema(StructType oldSchema) {
        return new WeightOfEvidenceModel()
                .setInputCols(this.getInputCols())
                .setOutputCols(this.getOutputCols()).transformSchema(oldSchema);
    }
    
    @Override
    public WeightOfEvidenceModel fit(Dataset<?> dataset) {
        SparkSession spark = dataset.sparkSession();
        String[] columns = this.getInputCols();
        String response  = this.getBooleanResponseCol();
        int minObs       = this.getMinObs();
        
        Map<String, Map<String, Double>> results = new HashMap<String, Map<String, Double>>();
        
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            
            dataset.createOrReplaceTempView("dataset");
        
            // Try restricting
            Dataset<Row> subTable = spark.sql(
                    " SELECT "
                    + " " + column + " AS currValue"
                    + " , SUM(CASE WHEN " + response + "= 0 THEN 1 ELSE 0 END) AS goods "
                    + " , SUM(CASE WHEN " + response + "= 1 THEN 1 ELSE 0 END) AS bads "
                    + " , COUNT(*) AS Cnt "
                    + " FROM dataset GROUP BY " + column);
            subTable.createOrReplaceTempView("subTable");
            
            // Group all < minObs into one large category
            Dataset<Row> othersTable = spark.sql(
                    " SELECT "
                    + " 'OtherBin' AS currValue"
                    + " , SUM(goods) AS goods "
                    + " , SUM(bads)  AS bads "
                    + " FROM subTable WHERE Cnt < " + minObs);
            othersTable.createOrReplaceTempView("othersTable");
            
            // Final
            Dataset<Row> finalTable = spark.sql(
                    " SELECT "
                    + " currValue"
                    + " , CASE WHEN bads = 0 AND goods = 0 THEN NULL "
                    + "        WHEN bads = 0 THEN log(goods/1) "
                    + "        WHEN goods = 0 THEN log(1/bads) "
                    + "        ELSE log(goods/bads) END AS woe "
                    + " FROM subTable WHERE Cnt >= " + minObs + " UNION ALL "
                    + " SELECT "
                    + "  currValue "
                    + " , CASE WHEN bads = 0 AND goods = 0 THEN NULL "
                    + "        WHEN bads = 0 THEN log(goods/1) "
                    + "        WHEN goods = 0 THEN log(1/bads) "
                    + "        ELSE log(goods/bads) END AS woe "
                    + " FROM othersTable ");

            List<Row> values = finalTable.collectAsList();
            
            results.put(column, new HashMap<String, Double>());
            
            for (Row currRow : values) {
                String currValue = currRow.getString(0);
                currValue = currValue == null ? "null" : currValue;
                Double woe = (Double) currRow.get(1);
                results.get(column).put(currValue, woe);
            }
        }
        
        WeightOfEvidenceModel woem = new WeightOfEvidenceModel()
                .setInputCols(this.getInputCols())
                .setOutputCols(this.getOutputCols())
                .setLookup(results);
        return woem;
    }

    @Override
    public Estimator<WeightOfEvidenceModel> copy(ParamMap arg0) {
        WeightOfEvidence copied = new WeightOfEvidence()
                .setInputCols(this.getInputCols())
                .setOutputCols(this.getOutputCols())
                .setMinObs(this.getMinObs());
        return copied;
    }

    @Override
    public MLWriter write() {
        return new WeightOfEvidenceWriter(this);
    }

    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }
    
    public static MLReader<WeightOfEvidence> read() {
        return new WeightOfEvidenceReader();
    }
    
    public WeightOfEvidence load(String path) {
        return ((WeightOfEvidenceReader)read()).load(path);
    }
    
    public void com$mrsharky$spark$ml$feature$WeightOfEvidence$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }
    
    public void com$mrsharky$spark$ml$feature$WeightOfEvidence$_setter_$outputCols_$eq(StringArrayParam stringArrayParam) {
        this._outputCols = stringArrayParam;
    }
    
    public void com$mrsharky$spark$ml$feature$WeightOfEvidence$_setter_$booleanResponseCol_$eq(Param<String> stringArrayParam) {
        this._booleanResponseCol = stringArrayParam;
    }
      
    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Name of columns to be processed");
    }
    
    public StringArrayParam outputCols() {
        return new StringArrayParam(this, "outputCols", "Name of columns for results");
    }
    
    public Param<String> booleanResponseCol() {
        return new Param<String>(this, "booleanResponse", "Boolean response variable column");
    }
    
    public IntParam minObs() {
        return new IntParam(this, "minObs", "Minimum number of observations to keep value");
    }
}
