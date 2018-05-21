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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.util.DefaultParamsWritable;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 *
 * @author Julien Pierret
 */
public class DropColumns extends Transformer implements Serializable, DefaultParamsWritable {

    private StringArrayParam _inputCols;
    private final String _uid;
    
    public DropColumns(String uid) {
        _uid = uid;
    }

    public DropColumns() {
        _uid = DropColumns.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    // Getters
    public String[] getInputCols() { return get(_inputCols).get(); }
       
    // Setters
    public DropColumns setInputCols(String[] columns) {
        _inputCols = inputCols();
        set(_inputCols, columns);
        return this;
    }
    
    public DropColumns setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray(new String[columns.size()]);
        return setInputCols(columnsString);
    }
    
    public DropColumns setInputCols(String column) {
        String[] columns = new String[]{column};
        return setInputCols(columns);
    }
    
    // Overrides
    @Override
    public Dataset<Row> transform(Dataset<?> data) {
        List<String> dropCol = new ArrayList<String>();
        Dataset<Row> newData = null;
        try {
            for (String currColumn : this.get(_inputCols).get() ) {
                dropCol.add(currColumn);
            }
            Seq<String> seqCol = JavaConverters.asScalaIteratorConverter(dropCol.iterator()).asScala().toSeq();      
            newData = data.drop(seqCol);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return newData;
    }

    @Override
    public Transformer copy(ParamMap extra) {
        DropColumns copied = new DropColumns();
        copied.setInputCols(this.getInputCols());
        return copied;
    }

    @Override
    public StructType transformSchema(StructType oldSchema) {
        StructField[] fields = oldSchema.fields();  
        List<StructField> newFields = new ArrayList<StructField>();
        List<String> columnsToRemove = Arrays.asList( get(_inputCols).get() );
        for (StructField currField : fields) {
            String fieldName = currField.name();
            if (!columnsToRemove.contains(fieldName)) {
                newFields.add(currField);
            }
        }
        StructType schema = DataTypes.createStructType(newFields);
        return schema;
    }

    @Override
    public String uid() {
        return _uid;
    }
    
    @Override
    public MLWriter write() {
        return new DropColumnsWriter(this);
    }

    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }
    
    public static MLReader<DropColumns> read() {
        return new DropColumnsReader();
    }

    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }
    
    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Columns to be dropped");
    }
    
    public DropColumns load(String path) {
        return ( (DropColumnsReader) read()).load(path);
    }
}