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

import static com.mrsharky.spark.utilities.SparkUtils.ListStringToSeqColumn;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Column;
import org.apache.spark.storage.StorageLevel;
import org.javatuples.Pair;
import scala.collection.Seq;

/**
 *
 * @author Julien Pierret
 */
public class SmartUnion {
    
    private Pair<Dataset<Row>, Integer> _finalData;
    
    private Dataset<Row> getUnionedDataset() {
        return _finalData.getValue0();
    }
    
    private int getPartitionCount() {
        return this._finalData.getValue1();
    }
    
    private Pair<Dataset<Row>, Integer> getDatasetAndPartitionCount() {
        return this._finalData;
    }
    
    public SmartUnion(List<Dataset<Row>> inputs) {
        // Get the column information for each input
        int totalNumPartitions = 0;
        List<String> allColumns = new ArrayList<String>();
        List<Map<String, DataType>> info = new ArrayList<Map<String, DataType>>();
        for (int i = 0; i < inputs.size(); i++) {
            Dataset<Row> input = inputs.get(i);
            totalNumPartitions = totalNumPartitions + input.javaRDD().getNumPartitions();
            StructType struct = input.schema();
            StructField[] fields = struct.fields();
            info.add(new HashMap<String, DataType>());
            
            // Loop through all the fields
            for (int fieldCounter = 0; fieldCounter < fields.length; fieldCounter++) {
                StructField currField = fields[fieldCounter];
                String fieldName = currField.name();
                DataType fieldType = currField.dataType();
                info.get(i).put(fieldName, fieldType);
                
                if (!allColumns.contains(fieldName)) {
                    allColumns.add(fieldName);
                }
            }
        }
        
        // Get the final List of fields common to each dataset
        List<String> finalColumns = new ArrayList<String>();
        for (int i = 0; i < allColumns.size(); i++) {
            String column = allColumns.get(i);
            DataType currDataType = null;
            DataType prevDataType = null;
            boolean addColumn = true;
            for (int j = 0; j < inputs.size(); j++) {
                // if column doesn't exist in all inputs, we can't add it
                if (!info.get(j).containsKey(column)) {
                    addColumn = false;
                    break;
                }
                
                // if columns don't have the same datatype across all we can't add it
                currDataType = info.get(j).get(column);
                if (prevDataType == null) {
                    prevDataType = currDataType;
                }
                
                if (!currDataType.equals(prevDataType)) {
                    addColumn = false;
                    break;
                }
                prevDataType = currDataType;
            }
                  
            if (addColumn) {
                finalColumns.add(column);
            }
        }
        
        Seq<Column> seqColumns = ListStringToSeqColumn(finalColumns);
        // Union it all together
        
        Dataset<Row> finalData = null;
        for (int i = 0; i < inputs.size(); i++) {
            Dataset<Row> input = inputs.get(i);   
            if (finalData == null) {
                finalData = input.select(seqColumns);
            } else {
                finalData = finalData.unionAll(input.select(seqColumns));
            }
        }
        finalData.persist(StorageLevel.DISK_ONLY());
        _finalData = Pair.with(finalData, totalNumPartitions);
    }
    
}
