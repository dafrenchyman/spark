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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Julien Pierret
 */
public class TopCategoriesFlatMap implements Serializable, FlatMapFunction<Iterator<Row>, Row> {
    
    private final Map<String, List<Object>> _elementRestrictions;
    private final StructType _structType;
    
    public TopCategoriesFlatMap (Map<String, List<Object>> elementRestrictions, StructType structType) {
        _elementRestrictions = elementRestrictions;
        _structType = structType;
    }
    
    @Override
    public Iterator<Row> call(Iterator<Row> rows) {
        List<Row> returnRows = new ArrayList<Row>();
        while (rows.hasNext()) {
            Row row = rows.next();
        
            try {
                List<Object> rowValues = new ArrayList<Object>();
                StructField[] structFields = _structType.fields();
                for (int structCounter = 0; structCounter < structFields.length; structCounter++ ) {
                    String fieldName = structFields[structCounter].name();
                    Object currValue = row.get(structCounter);

                    if (_elementRestrictions.containsKey(fieldName)) {
                        if (_elementRestrictions.get(fieldName).contains(currValue)) {
                            if (currValue == null) {
                                rowValues.add("Other");
                            } else {
                                rowValues.add(currValue);
                            }
                        } else {
                            rowValues.add("Other");
                        }
                    } else {
                        rowValues.add(currValue);
                    } 
                }
                returnRows.add(RowFactory.create(rowValues.toArray()));
            } catch (Exception ex) {
                System.err.println("ERROR - Unable to process Categorical Restriction for row");
                System.err.println(ex.getMessage());
                Logger.getLogger(TopCategoriesFlatMap.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return returnRows.iterator();
    }
}
