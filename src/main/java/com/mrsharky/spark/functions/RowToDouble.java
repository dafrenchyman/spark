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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

/**
 *
 * @author mrsharky
 */
public class RowToDouble implements Serializable, FlatMapFunction<Iterator<Row>, Double> {
    
    @Override
    public Iterator<Double> call(Iterator<Row> r) throws Exception {
        List<Double> rows = new ArrayList<Double>();
        while (r.hasNext()) {
            Row s = r.next();
            Object value = s.get(0);
            Double val = null; 
            if (value != null) {
                if (value.getClass().equals(Double.class)) {
                    val = s.getDouble(0);
                } else if (value.getClass().equals(Integer.class)) {
                    val = (double) s.getInt(0);
                } else if (value.getClass().equals(Float.class)) {
                    val = (double) s.getFloat(0);
                } else if (value.getClass().equals(Long.class)) {
                    val = (double) s.getLong(0);
                } else if (value.getClass().equals(Short.class)) {
                    val = (double) s.getShort(0);
                }
            }
            rows.add(val);
        }
        return rows.iterator();
    }
}
