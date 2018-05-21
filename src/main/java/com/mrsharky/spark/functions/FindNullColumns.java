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

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;

/**
 *
 * @author Julien Pierret
 */
public class FindNullColumns {
    
    private final List<String> _alwaysNullColumns;
    
    public List<String> getNullColumnsAsListString() {
        return this._alwaysNullColumns;
    }
    
    public List<Column> getNullColumnAsListColumn() {
        List<Column> output = new ArrayList<Column>();
        for (String currCol : _alwaysNullColumns) {
            output.add(new Column(currCol));
        }
        return output;
    }
    
    public FindNullColumns (Dataset<Row> inputData) {
        StructField[] fields = inputData.schema().fields();
        
        // Check which columns are strings that contain only numbers
        FindNullColumns_Mapper mtin = new FindNullColumns_Mapper();
        JavaRDD<Row> testIfNumber = inputData.toJavaRDD().mapPartitions(c -> mtin.call(c));
        FindNullColumns_Combiner mtinc = new FindNullColumns_Combiner();
        Row result = testIfNumber.aggregate(RowFactory.create(), mtinc, mtinc);
        
        // Find the columns that where always NULL
        List<String> allwaysNullColumns = new ArrayList<String>();
        for (int counter = 0; counter < fields.length; counter++) {
            StructField currField = fields[counter];
            String currColumn = currField.name();
            if (result.getBoolean(counter)) {
                allwaysNullColumns.add(currColumn);
            }
        }
        _alwaysNullColumns = allwaysNullColumns;
    }
}
