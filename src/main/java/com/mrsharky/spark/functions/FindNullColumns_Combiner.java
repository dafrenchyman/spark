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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 *
 * @author Julien Pierret
 */
public class FindNullColumns_Combiner implements Serializable, Function2<Row, Row, Row> {
    
    public FindNullColumns_Combiner () {
    }

    @Override
    public Row call(Row v1, Row v2)  {
        Boolean[] finalValues = new Boolean[0];
        int size = Math.max(v1.length(), v2.length());
        if (v1.size() > 0 && v2.size() > 0) {
            finalValues = new Boolean[v1.length()];
            
            for (int counter = 0; counter < v1.length(); counter++) {
                boolean value1 = v1.getBoolean(counter);
                boolean value2 = v2.getBoolean(counter);
                if (value1 == false || value2 == false) {
                    finalValues[counter] = false;
                } else {
                    finalValues[counter] = true;
                }
            }
        } else if (v1.size() > 0) {
            finalValues = new Boolean[v1.length()];
            for (int counter = 0; counter < v1.length(); counter++) {
                finalValues[counter] = v1.getBoolean(counter);
            }
        } else if (v2.size() > 0) {
            finalValues = new Boolean[v2.length()];
            for (int counter = 0; counter < v2.length(); counter++) {
                finalValues[counter] = v2.getBoolean(counter);
            }
        }
        return RowFactory.create(finalValues);       
    }
}
