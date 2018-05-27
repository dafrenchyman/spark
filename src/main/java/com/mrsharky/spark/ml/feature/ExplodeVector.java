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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.IntArrayParam;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.util.DefaultParamsWritable;
import org.javatuples.Pair;

/**
 *
 * @author Julien Pierret
 */
public class ExplodeVector extends Transformer implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _inputCols;
    private IntArrayParam _inputColsSize;
    private final String _uid;
    
    public ExplodeVector(String uid) {
        _uid = uid;
    }

    public ExplodeVector() {
        _uid = ExplodeVector.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    public ExplodeVector setInputCols(Pair<String, Integer> columnInfo) {
        String[] names = new String[1];
        int[] sizes = new int[1];

        names[0] = columnInfo.getValue0();
        sizes[0] = columnInfo.getValue1();
        
        _inputCols = inputCols();
        _inputColsSize = inputColsSize();
        set(_inputCols, names);
        set(_inputColsSize, sizes);
        return this;
    }
    
    public ExplodeVector setInputCols(List<Pair<String, Integer>> columnInfo) {
        String[] names = new String[columnInfo.size()];
        int[] sizes = new int[columnInfo.size()];
        for (int i = 0; i < columnInfo.size(); i++) {
            names[i] = columnInfo.get(i).getValue0();
            sizes[i] = columnInfo.get(i).getValue1();
        }
        _inputCols = inputCols();
        _inputColsSize = inputColsSize();
        set(_inputCols, names);
        set(_inputColsSize, sizes);
        return this;
    }
    
    public String[] getInputCols() {
        return get(_inputCols).get();
    }
    
    public int[] getInputColsSize() {
        return get(_inputColsSize).get();
    }
    
    public List<Pair<String, Integer>> getInputColsPair() {
        List<Pair<String, Integer>> pairs = new ArrayList<Pair<String, Integer>>();
        String[] inputCols = this.getInputCols(); 
        int[] inputColsSize = this.getInputColsSize();
        for (int i = 0; i < inputCols.length; i++) {
            pairs.add(Pair.with(inputCols[i], inputColsSize[i]));
        }
        return pairs;
    }
    
    private Map<String, Integer> generateColumnsToExplode() {
        String[] columnsToExplodeName = this.get(_inputCols).get();
        int[] columnsToExplodeSize = this.get(_inputColsSize).get();
        Map<String, Integer> columnsToExplode = new HashMap<String, Integer>();
        for (int i = 0; i < columnsToExplodeName.length; i++) {
            String currName = columnsToExplodeName[i];
            int currSize = columnsToExplodeSize[i];
            columnsToExplode.put(currName, currSize);
        }
        return columnsToExplode;
    }
    
    @Override
    public Dataset<Row> transform(Dataset<?> data) {
        Dataset<Row> newData = null;
        StructType structType = data.schema();     
        Map<String, Integer> columnsToExplode = generateColumnsToExplode(); 
        try {
            ExplodeVectorFlatMap evfm = new ExplodeVectorFlatMap(columnsToExplode);
            newData = data.mapPartitions(f -> evfm.call((Iterator<Row>) f), evfm.getEncoder(structType));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return newData;
    }

    @Override
    public Transformer copy(ParamMap extra) {
        ExplodeVector copied = new ExplodeVector();
        copied.setInputCols( getInputColsPair() );
        return copied;
    }

    @Override
    public StructType transformSchema(StructType oldSchema) {
        StructType schema = null;
        try {
            Map<String, Integer> columnsToExplode = generateColumnsToExplode();
            ExplodeVectorFlatMap evfm = new ExplodeVectorFlatMap(columnsToExplode);
            schema = evfm.getSchema(oldSchema);
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
        return new ExplodeVectorWriter(this);
    }

    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }
    
    public static MLReader<ExplodeVector> read() {
        return new ExplodeVectorReader();
    }

    public void com$mrsharky$spark$ml$feature$ExplodeVector$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }
    
    public void com$mrsharky$spark$ml$feature$ExplodeVector$_setter_$inputColsSize_$eq(IntArrayParam intArrayParam) {
        this._inputColsSize = intArrayParam;
    }
    
    
    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Columns to 'Explode'");
    }
    
    public IntArrayParam inputColsSize() {
        return new IntArrayParam(this, "inputColsSize", "Number of vector elements in column");
    }

    public ExplodeVector load(String path) {
        return ( (ExplodeVectorReader) read()).load(path);
    }
}
