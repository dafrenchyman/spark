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
package com.mrsharky.spark.functions;

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

/**
 *
 * @author Julien Pierret
 */
public class PivotFieldsFromRowCount implements Serializable, FlatMapFunction<Iterator<Row>, Row> {

    private final List<String> _variables;
    private Map<String, Map<Object, Long>> _index;
    private StructType _structType;
    
    public PivotFieldsFromRowCount(List<String> variables) {
        this._variables = variables;
        this._index = new HashMap<String, Map<Object, Long>>();
        this._structType = null;
    }
    
    public PivotFieldsFromRowCount(List<String> variables, StructType structType) {
        this._variables = variables;
        this._index = new HashMap<String, Map<Object, Long>>();
        this._structType = structType;
    }
    
    @Override
    public Iterator<Row> call(Iterator<Row> linesPerPartition) throws Exception {
        List<Row> returnRows = new ArrayList<Row>();
        long rowsIn = 0;
        System.out.println(PivotFieldsFromRowCount.class.getName() + " - Starting");
        Map<String, Integer> variableMap = new HashMap<String, Integer>();
        while (linesPerPartition.hasNext()) {
            Row currRow = linesPerPartition.next();
            if (rowsIn == 0) {
                StructField[] fields = null;
                if (this._structType != null) {
                    fields = this._structType.fields();
                } else {
                    fields = currRow.schema().fields();
                }
                for (int fieldCounter = 0; fieldCounter < fields.length; fieldCounter++) {
                    StructField currField = fields[fieldCounter];
                    if (this._variables.contains(currField.name())) {
                        variableMap.put(currField.name(), fieldCounter);
                    }
                }
            }
            
            for (String currVariable : this._variables) {
                Object currValue = currRow.get(variableMap.get(currVariable));
                
                if (!this._index.containsKey(currVariable)) {
                    this._index.put(currVariable, new HashMap<Object, Long>());
                }
                if (!this._index.get(currVariable).containsKey(currValue)){
                    this._index.get(currVariable).put(currValue, 1L);
                } else {
                    long currCount = this._index.get(currVariable).get(currValue);
                    this._index.get(currVariable).put(currValue, currCount + 1L);
                }
            }
            if (rowsIn % 20000 == 0) {
                System.out.println(PivotFieldsFromRowCount.class.getName() + " - Running: " + rowsIn);
            }
            rowsIn++;
        }
        
        long rowsOut = 0;
        for (String currVariable : _index.keySet()) {
            for (Object currValue : _index.get(currVariable).keySet()) {
                long currCount = _index.get(currVariable).get(currValue);
                returnRows.add(RowFactory.create(currVariable, currValue, currCount));
                rowsOut++;
            }
        }
        
        System.out.println(PivotFieldsFromRowCount.class.getName() + " - Finished (RowsIn: " + rowsIn + ", RowsOut: "+ rowsOut + ")");
        return returnRows.iterator();  
    }
}
