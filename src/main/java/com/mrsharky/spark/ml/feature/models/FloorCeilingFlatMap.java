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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.javatuples.Pair;

/**
 *
 * @author Julien Pierret
 */
public class FloorCeilingFlatMap implements Serializable, FlatMapFunction<Iterator<Row>, Row> {

    private final Map<Integer, Pair<Double, Double>> _lookup;
    private final Map<String, Pair<Double, Double>>  _columnsToProcess;
     
    public FloorCeilingFlatMap(Map<String, Pair<Double, Double>> columnsToProcess, StructType structType) throws Exception {
        _columnsToProcess = columnsToProcess;
        _lookup = generateLookup(_columnsToProcess, structType);
    }
    
    private Map<Integer, Pair<Double, Double>> generateLookup(Map<String, Pair<Double, Double>> columnsToProcess, StructType structType) {
        Map<Integer, Pair<Double, Double>> lookups = new HashMap<Integer, Pair<Double, Double>>();

        // Generate the Lookup key based off column index
        StructField[] currFields = structType.fields(); 
        for (int i = 0; i < currFields.length; i++) {
            StructField currField = currFields[i];
            String fieldName = currField.name();
            if (columnsToProcess.containsKey(fieldName)) {
                Pair<Double, Double> pair = columnsToProcess.get(fieldName);
                lookups.put(i, pair);
            }
        }
        return lookups;
    }
    

    @Override
    public Iterator<Row> call(Iterator<Row> rows) throws Exception {
        List<Row> outputRows = new ArrayList<Row>();
        outputRows = process(rows);
        return outputRows.iterator();
    }
    
    private List<Row> process(Iterator<Row> rows) {
        List<Row> outputRows = new ArrayList<Row>();
        while (rows.hasNext()) {
            Row currRow = rows.next();

            List<Object> rowElements = new ArrayList<Object>();
            for (int i = 0; i < currRow.size(); i++) {
                
                if (_lookup.containsKey(i)) {
                    double lowerBound = _lookup.get(i).getValue0();
                    double upperBound = _lookup.get(i).getValue1();
                    Object value = currRow.get(i);
                    
                    if (value.getClass().equals(Double.class)) {
                        double oldValue = currRow.getDouble(i);
                        double newValue = oldValue < lowerBound ? lowerBound : 
                                          oldValue > upperBound ? upperBound : oldValue;
                        rowElements.add(newValue);
                    } else if (value.getClass().equals(Integer.class)) {
                        int oldValue = currRow.getInt(i);
                        int newValue = oldValue < (int) lowerBound ? (int) lowerBound : 
                                       oldValue > (int) upperBound ? (int) upperBound : oldValue;
                        rowElements.add(newValue);
                    } else if (value.getClass().equals(Long.class)) {
                        long oldValue = currRow.getLong(i);
                        long newValue = oldValue < (long) lowerBound ? (long) lowerBound : 
                                        oldValue > (long) upperBound ? (long) upperBound : oldValue;
                        rowElements.add(newValue);
                    } else if (value.getClass().equals(Float.class)) {
                        float oldValue = currRow.getFloat(i);
                        float newValue = oldValue < (float) lowerBound ? (float) lowerBound : 
                                         oldValue > (float) upperBound ? (float) upperBound : oldValue;
                        rowElements.add(newValue);
                    } else if (value.getClass().equals(Short.class)) {
                        short oldValue = currRow.getShort(i);
                        short newValue = oldValue < (short) lowerBound ? (short) lowerBound : 
                                         oldValue > (short) upperBound ? (short) upperBound : oldValue;
                        rowElements.add(newValue);
                    } else {
                        rowElements.add(currRow.get(i));
                    }
                } else {
                    rowElements.add(currRow.get(i));
                }
            }
            Row newRow = RowFactory.create(rowElements.toArray());
            outputRows.add(newRow);
        }
        return outputRows;
    }
}