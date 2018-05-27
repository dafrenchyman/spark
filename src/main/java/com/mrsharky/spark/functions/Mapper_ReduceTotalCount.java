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

import com.mrsharky.utilites.MapUtil;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 *
 * @author Julien Pierret
 */
public class Mapper_ReduceTotalCount implements Serializable, FlatMapFunction<Iterator<Row>, Row> {
    
    private final Map<String, Map<Object, Long>> _index;
    private final int _maxTotal;
    private final Integer _totalColumnLocation;
    
    public Mapper_ReduceTotalCount (int maxTotal) {
        _index = new HashMap<String, Map<Object, Long>>();
        // doubling the maxTotal (not perfect) as maybe some the "max" ones are more spread out across the different partitions
        _maxTotal = maxTotal*2; 
        _totalColumnLocation = null;
    }
    
    public Mapper_ReduceTotalCount (int maxTotal, int totalColumnLocation) {
        _index = new HashMap<String, Map<Object, Long>>();
        _maxTotal = maxTotal*2;
        _totalColumnLocation = totalColumnLocation;
    }
    
    @Override
    public Iterator<Row> call(Iterator<Row> lineIterator) {      
        List<Row> rows = new ArrayList<Row>();
        System.out.println(Mapper_ReduceTotalCount.class.getName() + " - Starting");
        int rowsIn = 0;
        while (lineIterator.hasNext()) {
            Row newLine = lineIterator.next();
            
            String varName = newLine.getString(0);
            Object varValue = newLine.get(1);
            long totalCount = 1L;
            if (_totalColumnLocation != null) {
                totalCount = newLine.getLong(this._totalColumnLocation);
            }
            
            if (!_index.containsKey(varName)) {
                _index.put(varName, new HashMap<Object, Long>());
            }
            
            if (!_index.get(varName).containsKey(varValue)) {
                _index.get(varName).put(varValue, totalCount);
            } else {
                long currCount = _index.get(varName).get(varValue);
                _index.get(varName).put(varValue, currCount + 1L);
            }
            rowsIn++;
            
            if (rowsIn % 10000 == 0) {
                System.out.println(Mapper_ReduceTotalCount.class.getName() + " - Processing: " + rowsIn);
            }
        }
        
        // Loop through each variable
        int rowsOut = 0;
        for (String varName : this._index.keySet()) {
            Map<Object, Long> orderedVar = MapUtil.sortByValue(_index.get(varName), false);
            int exitCounter = 0;
            for (Object currValue : orderedVar.keySet()) {
                rows.add(RowFactory.create(varName, currValue, orderedVar.get(currValue)));
                rowsOut++;
                exitCounter++;
                if (exitCounter > _maxTotal) {
                    break;
                }
            }   
        }
        System.out.println(Mapper_ReduceTotalCount.class.getName() + " - Finished "
                + "(RowsIn: " + rowsIn + ", RowsOut: " + rowsOut + ")");
        return rows.iterator();  
    }
}
