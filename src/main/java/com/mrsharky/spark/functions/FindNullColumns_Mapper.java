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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;

/**
 *
 * @author Julien Pierret
 */
public class FindNullColumns_Mapper implements Serializable, FlatMapFunction<Iterator<Row>, Row> {
    
    public FindNullColumns_Mapper () {
    }
    
    @Override
    public Iterator<Row> call(Iterator<Row> lineIterator) {      
        List<Row> rows = new ArrayList<Row>();
        System.out.println(FindNullColumns_Mapper.class.getName() + " - Starting");      
        long counter = 0;
        StructField[] structFields = null;
        List<Integer> fieldsToProcess = null;
        Boolean[] finalValuesAreNull = new Boolean[0];
        while (lineIterator.hasNext()) {
            Row newLine = lineIterator.next();
            
            if (counter == 0) {
                structFields = newLine.schema().fields();
                finalValuesAreNull = new Boolean[structFields.length];
                fieldsToProcess = new ArrayList<Integer>();
                
                for (int structCounter = 0; structCounter < structFields.length; structCounter++ ) {
                    fieldsToProcess.add(structCounter);
                    finalValuesAreNull[structCounter] = true;
                }
            }
            counter++;
            
            // loop through each element
            Iterator<Integer> itr = fieldsToProcess.iterator();
            while(itr.hasNext()) {
                Integer currField = itr.next();
                Object currValue = newLine.get(currField);
                if (currValue != null) {
                    finalValuesAreNull[currField] = false;
                    itr.remove();
                }
            }
            
            if (counter % 10000 == 0) {
                System.out.println(FindNullColumns_Mapper.class.getName() + " - Processing: " + counter);
            }
        }
        System.out.println(FindNullColumns_Mapper.class.getName() + " - Finished: " + counter);
        rows.add(RowFactory.create(finalValuesAreNull));
        return rows.iterator();  
    }
}
