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
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import scala.Tuple2;


/**
 *
 * @author Julien Pierret
 */
public class FlatmapToPairWithCounts implements Serializable, PairFlatMapFunction<Row, Row, Row> {

    private final List<Integer> _mainKey;
    private final int _totalCountLocation;

    
    public FlatmapToPairWithCounts (List<Integer> mainKey, int totalCountLocation) {
        this._mainKey = mainKey;
        this._totalCountLocation = totalCountLocation;
    }

    @Override
    public Iterator<Tuple2<Row, Row>> call(Row currRow)  {        
        List<Tuple2<Row, Row>> ret = new ArrayList<Tuple2<Row, Row>>();
        
        String[] key = new String[_mainKey.size()];
        for (int loc = 0; loc < this._mainKey.size(); loc++) {
            key[loc] = (String) currRow.get(this._mainKey.get(loc));
        }
        
        List<Object> list = new ArrayList<Object>();     
        list.add(currRow.get(_totalCountLocation));
        list.add(1L);
        
        GenericRow newRow = new GenericRow(key);
        Tuple2<Row,Row> tuple = new Tuple2<Row, Row>(newRow, RowFactory.create(list.toArray()));
        ret.add(tuple);
  
        return ret.iterator();
    }
}
