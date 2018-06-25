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
import org.javatuples.Triplet;

/**
 *
 * @author Julien Pierret
 */
public class FloorCeilingMissingFlatMap implements Serializable, FlatMapFunction<Iterator<Row>, Row> {

    private final Map<Integer, Triplet<Double, Double, Double>> _lookup;
    private final Map<String, Triplet<Double, Double, Double>>  _columnsToProcess;
     
    public FloorCeilingMissingFlatMap(Map<String, Triplet<Double, Double, Double>> columnsToProcess, StructType structType) throws Exception {
        _columnsToProcess = columnsToProcess;
        _lookup = generateLookup(_columnsToProcess, structType);
    }
    
    private Map<Integer, Triplet<Double, Double, Double>> generateLookup(Map<String, Triplet<Double, Double, Double>> columnsToProcess, StructType structType) {
        Map<Integer, Triplet<Double, Double, Double>> lookups = new HashMap<Integer, Triplet<Double, Double, Double>>();

        // Generate the Lookup key based off column index
        StructField[] currFields = structType.fields(); 
        for (int i = 0; i < currFields.length; i++) {
            StructField currField = currFields[i];
            String fieldName = currField.name();
            if (columnsToProcess.containsKey(fieldName)) {
                Triplet<Double, Double, Double> triplet = columnsToProcess.get(fieldName);
                lookups.put(i, triplet);
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
                    Double lowerBound   = _lookup.get(i).getValue0();
                    Double upperBound   = _lookup.get(i).getValue1();
                    Double missingBound = _lookup.get(i).getValue2();
                    Object value = currRow.get(i);
                    
                    if (value.getClass().equals(Double.class)) {
                        Double newValue = null;
                        if (currRow.isNullAt(i)) {
                            if (missingBound != null) {
                                newValue = missingBound;
                            }
                        } else {
                            double oldValue = currRow.getDouble(i);
                            newValue = lowerBound != null && oldValue < lowerBound ? lowerBound :
                                       upperBound != null && oldValue > upperBound ? upperBound : oldValue;
                        }          
                        rowElements.add(newValue);
                    } else if (value.getClass().equals(Integer.class)) {
                        Integer newValue = null;
                        if (currRow.isNullAt(i)) {
                            if (missingBound != null) {
                                newValue = missingBound.intValue();
                            }
                        } else {
                            int oldValue = currRow.getInt(i);
                            newValue = lowerBound != null && oldValue < lowerBound.intValue() ? lowerBound.intValue() :
                                       upperBound != null && oldValue > upperBound.intValue() ? upperBound.intValue() : oldValue;
                        }
                        rowElements.add(newValue);
                    } else if (value.getClass().equals(Long.class)) {
                        Long newValue = null;
                        if (currRow.isNullAt(i)) {
                            if (missingBound != null) {
                                newValue = missingBound.longValue();
                            }
                        } else {
                            long oldValue = currRow.getLong(i);
                            newValue = lowerBound != null && oldValue < lowerBound.longValue() ? lowerBound.longValue() :
                                       upperBound != null && oldValue > upperBound.longValue() ? upperBound.longValue() : oldValue;
                        }
                        rowElements.add(newValue);
                    } else if (value.getClass().equals(Float.class)) {
                        Float newValue = null;
                        if (currRow.isNullAt(i)) {
                            if (missingBound != null) {
                                newValue = missingBound.floatValue();
                            }
                        } else {
                            float oldValue = currRow.getFloat(i);
                            newValue = lowerBound != null && oldValue < lowerBound.floatValue() ? lowerBound.floatValue() : 
                                       upperBound != null && oldValue > upperBound.floatValue() ? upperBound.floatValue() : oldValue;
                        }
                        rowElements.add(newValue);
                    } else if (value.getClass().equals(Short.class)) {
                        Short newValue = null;
                        if (currRow.isNullAt(i)) {
                            if (missingBound != null) {
                                newValue = missingBound.shortValue();
                            }
                        } else {
                            short oldValue = currRow.getShort(i);
                            newValue = lowerBound != null && oldValue < lowerBound.shortValue() ? lowerBound.shortValue() : 
                                       upperBound != null && oldValue > upperBound.shortValue() ? upperBound.shortValue() : oldValue;
                        }
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