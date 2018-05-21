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

import static com.mrsharky.spark.utilities.SparkUtils.GetPipeline;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.util.DefaultParamsWritable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 *
 * @author Julien Pierret
 */
public class MultiStringIndexer extends Estimator<PipelineModel> implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _inputCols;
    private StringArrayParam _outputCols;
    private String _uid;
    
   
    @Override
    public String uid() {
        return _uid;
    }
    
    public MultiStringIndexer(String uid) {
        _uid = uid;
    }

    public MultiStringIndexer() {
        _uid = MultiStringIndexer.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    public String[] getInputCols() {
        return this.get(_inputCols).get();
    }
    
    public String[] getOutputCols() {
        return this.get(_outputCols).get();
    }
    
    public MultiStringIndexer setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray(new String[columns.size()]);
        return setInputCols(columnsString);
    }
    
    public MultiStringIndexer setInputCols(String[] inputs) {
        String[] outputs = new String[inputs.length];
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = inputs[i] + "_idx";
        }
        _inputCols = inputCols();
        _outputCols = outputCols();
        this.set(_inputCols, inputs);
        this.set(_outputCols, outputs);
        return this;
    }
    
    public MultiStringIndexer setOutputCols(List<String> outputs) {
        String[] columnsString = outputs.toArray(new String[outputs.size()]);
        _outputCols = outputCols();
        this.set(_outputCols, columnsString);
        return this;
    }
    
    public MultiStringIndexer setOutputCols(String[] outputs) {
        _outputCols = outputCols();
        this.set(_outputCols, outputs);
        return this;
    }
    
     
    @Override
    public StructType transformSchema(StructType oldSchema) {
        String[] inputCols  = this.getInputCols();
        String[] outputCols = this.getOutputCols();
        
        List<StructField> newFields = new ArrayList<StructField>();
        
        // Recreate the original schema first
        StructField[] fields = oldSchema.fields();
        for (int i = 0; i < fields.length; i++) {
            StructField currField = fields[i];
            String fieldName = currField.name();
            DataType currDataType = currField.dataType();
            boolean currNullable = currField.nullable();
            newFields.add(DataTypes.createStructField(fieldName, currDataType, currNullable));
        }
        
        // Add stuff to the end
        List<String> availableColumns = Arrays.asList(oldSchema.fieldNames());
        for (int i = 0; i < outputCols.length; i++) {
            String currIn  = inputCols[i];
            String currOut = outputCols[i];
            if (availableColumns.contains(currIn)) {
                newFields.add(DataTypes.createStructField(currOut, DataTypes.DoubleType, false));
            }
        }
        return DataTypes.createStructType(newFields);
    }
    
    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }
    
    public void org$apache$spark$ml$param$shared$HasOutputCols$_setter_$outputCols_$eq(StringArrayParam stringArrayParam) {
        this._outputCols = stringArrayParam;
    }
    
    @Override
    public PipelineModel fit(Dataset<?> dataset) {
        StructType structType = dataset.schema();

        List<PipelineStage> pipelineList = new ArrayList<PipelineStage>();
        
        String[] inputCols  = this.getInputCols();
        String[] outputCols = this.getOutputCols();
        List<String> availableColumns = Arrays.asList(structType.fieldNames());
        for (int i = 0; i < outputCols.length; i++) {
            String currIn  = inputCols[i];
            String currOut = outputCols[i];
            if (availableColumns.contains(currIn)) {
                StringIndexer indexer = new StringIndexer()
                        .setInputCol(currIn)
                        .setOutputCol(currOut);
                pipelineList.add(indexer);
            }
        }
        
        Pipeline pipeline = GetPipeline( pipelineList );
        PipelineModel pipelineModel = pipeline.fit(dataset);
        return pipelineModel;
    }

    @Override
    public Estimator<PipelineModel> copy(ParamMap arg0) {
        MultiStringIndexer copied = new MultiStringIndexer()
                .setInputCols(this.getInputCols())
                .setOutputCols(this.getOutputCols());
        return copied;
    }

    @Override
    public MLWriter write() {
        return new MultiStringIndexerWriter(this);
    }

    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }
    
    public static MLReader<MultiStringIndexer> read() {
        return new MultiStringIndexerReader();
    }
    
    public MultiStringIndexer load(String path) {
        return ((MultiStringIndexerReader)read()).load(path);
    }
    
    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Name of columns to be processed");
    }
    
    public StringArrayParam outputCols() {
        return new StringArrayParam(this, "outputCols", "Output column names");
    }
    
}
