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
import java.util.List;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 *
 * @author Julien Pierret
 */
public class UniqueCountsCombiner implements Serializable, Function2<Row, Row, Row> {
    
    public UniqueCountsCombiner () {
    }

    @Override
    public Row call(Row row1, Row row2) throws Exception {
        List<Object> list = new ArrayList<Object>();
        try {
            // total, unique
            long totalCounts = 0;   

            totalCounts += (long) row1.get(0);
            totalCounts += (long) row2.get(0);

            list.add(totalCounts);  // Total 
            list.add(1L);           // Unique
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getMessage());
            System.err.println("ERROR: " + ex.getMessage());
        }
        return RowFactory.create(list.toArray());
    }
}
