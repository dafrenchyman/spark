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
package com.mrsharky.spark.utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 *
 * @author Julien Pierret
 */
public class DatasetUtils {
    
    public static List<String> FindContinuousColumns(Dataset<Row> inputData) {
        StructField[] fields = inputData.schema().fields();
        
        // Loop through the columns to find numerical datatypes
        List<String> continuousColumns = new ArrayList<String>();
        for (int counter = 0; counter < fields.length; counter++) {
            StructField currField = fields[counter];
            String currColumn = currField.name();
            DataType currDataType = currField.dataType();
            if (    currDataType.equals(DataTypes.DoubleType)  ||
                    currDataType.equals(DataTypes.FloatType)   ||
                    currDataType.equals(DataTypes.IntegerType) ||
                    currDataType.equals(DataTypes.LongType)    ||
                    currDataType.equals(DataTypes.ShortType)) {
                continuousColumns.add(currColumn);
            }
        }
        return continuousColumns;
    }
    
    public static List<String> FindCategoricalColumns(Dataset<Row> inputData) {
        StructField[] fields = inputData.schema().fields();
        
        // Loop through the columns to find StringType columns
        List<String> categoricalColumns = new ArrayList<String>();
        for (int counter = 0; counter < fields.length; counter++) {
            StructField currField = fields[counter];
            String currColumn = currField.name();
            DataType currDataType = currField.dataType();
            if (currDataType.equals(DataTypes.StringType)) {
                categoricalColumns.add(currColumn);
            }
        }
        return categoricalColumns;
    }
    
    public static boolean ColumnExists(Dataset<Row> inputData, String columnName) {
        List<String> columns = Arrays.asList(inputData.columns());
        return columns.contains(columnName);
    }
}
