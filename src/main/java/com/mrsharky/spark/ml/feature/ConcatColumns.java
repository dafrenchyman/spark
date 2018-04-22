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
import java.util.List;
import java.util.UUID;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
//import org.apache.spark.ml.util.DefaultParamsWritable;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.functions;

/**
 *
 * @author Julien Pierret
 */
public class ConcatColumns extends Transformer implements Serializable, ConcatColumnsParams, MLWritable {
    
    private Param<String> _outputCol;
    private Param<String> _concatValue;
    private StringArrayParam _inputCols;
    private final String _uid;
    
    public ConcatColumns() {
        _uid = "ConcatColumns" + "_" + UUID.randomUUID().toString();
    }
    
    public ConcatColumns(String uid) {
        _uid = uid;
    }
    
    // Getters 
    public String[] getInputCols() { return get(_inputCols).get(); }
    public String getOutputCol()   { return get(_outputCol).get(); }
    public String getConcatValue() { return get(_concatValue).get(); }
    
    // Setters
    public ConcatColumns setConcatValue(String value) {
        _concatValue = concatValue();
        set(_concatValue, value);
        return this;
    }
    
    public ConcatColumns setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray( new String[columns.size()] );
        _inputCols = inputCols();
        set(_inputCols, columnsString);
        return this;
    }
    
    public ConcatColumns setInputCols(String[] columns) {
        _inputCols = inputCols();
        set(_inputCols, columns);
        return this;
    }
    
    public ConcatColumns setInputCols(String column) {
        String[] columns = new String[]{column};
        _inputCols = inputCols();
        set(_inputCols, columns);
        return this;
    }

    public ConcatColumns setOutputCol(String value) {
        _outputCol = outputCol();
        set(_outputCol, value);
        return this;
    }
    
    // Overrides
    @Override
    public Dataset<Row> transform(Dataset<?> data) {
        List<Column> newCol = new ArrayList<Column>();
        Dataset<Row> newData = null;
        try {
            String[] columns = get(_inputCols).get();
            for (int i = 0; i < columns.length; i++  ) {
                String currColumn = columns[i];
                newCol.add(functions.when(data.col(currColumn).isNull(), functions.lit("")).otherwise(data.col(currColumn)));
                if (i < columns.length -1) {
                    newCol.add(functions.lit(get(_concatValue).get()));
                }
            }
            Seq<Column> seqCol = JavaConverters.asScalaIteratorConverter(newCol.iterator()).asScala().toSeq();
            newData = data.withColumn( get(_outputCol).get(), functions.concat(seqCol) );
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return newData;
    }

    @Override
    public Transformer copy(ParamMap extra) {
        ConcatColumns copied = new ConcatColumns();
        copied.setInputCols(this.getInputCols());
        copied.setOutputCol(this.getOutputCol());
        return copied;
    }

    @Override
    public StructType transformSchema(StructType oldSchema) {
        StructType schema = null;
        StructField[] fields = oldSchema.fields();
        try {
            List<StructField> newFields = new ArrayList<StructField>();
            for (StructField currField : fields) {
                newFields.add(currField);
            }
            newFields.add(DataTypes.createStructField(this.get(_outputCol).get(), DataTypes.StringType, true));
            schema = DataTypes.createStructType(newFields);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return schema;
    }

    @Override
    public String uid() {
        return _uid;
    }
    
    @Override
    public MLWriter write() {
        return new ConcatColumnsWriter(this);
    }

    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }
    
    public static MLReader<ConcatColumns> read() {
        return new ConcatColumnsReader();
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasOutputCol$_setter_$outputCol_$eq(Param param) {
        this._outputCol = param;
    }
    
    @Override
    public void org$apache$spark$ml$param$shared$HasConcatValue$_setter_$concatValue_$eq(Param param) {
        this._concatValue = param;
    }
    
    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Columns to be concatenated togother");
    }

    public Param<String> outputCol() {
        return new Param<String>(this, "outputCol", "Name of output column");
    }
    
    public Param<String> concatValue() {
        return new Param<String>(this, "concatValue", "String value to split concats");
    }

    public ConcatColumns load(String path) {
        return ( (ConcatColumnsReader) read()).load(path);
    }
}
