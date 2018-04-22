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
package com.mrsharky.spark.ml.feature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Julien Pierret
 */
public class ExplodeVectorFlatMap implements Serializable, FlatMapFunction<Iterator<Row>, Row> {

    private Map<Integer, Integer> _elementsPerExpansion;
    private final Map<String, Integer> _columnsToExplode;
     
    
    public ExplodeVectorFlatMap(Map<String, Integer> columnsToExplode) throws Exception {
        _columnsToExplode = columnsToExplode;
    }
    
    public StructType generateSchemaSelectColumns(StructType structure) {
        _elementsPerExpansion = new HashMap<Integer, Integer>();
        List columnsToExplode = new ArrayList(_columnsToExplode.keySet());

        // Generate the Schema & find the columns to extract
        StructField[] currFields = structure.fields(); 
        List<StructField> newFields = new ArrayList<StructField>();
        
        for (int i = 0; i < currFields.length; i++) {
            StructField currField = currFields[i];
            String fieldName = currField.name();
            DataType currDataType = currField.dataType();
            boolean currNullable = currField.nullable();
            newFields.add(DataTypes.createStructField(fieldName, currDataType, currNullable));
            if (columnsToExplode.contains(fieldName) && currDataType.equals(new VectorUDT().defaultConcreteType())) {
                int vectorSize = _columnsToExplode.get(fieldName);
                _elementsPerExpansion.put(i, vectorSize);
                for (int j = 0; j < vectorSize; j++) {
                    String newFieldName = fieldName + "_" + j;
                    newFields.add(DataTypes.createStructField(newFieldName, DataTypes.DoubleType, true));
                }
            }
        }
        return DataTypes.createStructType(newFields);
    }
    
    public ExplodeVectorFlatMap(Row row) throws Exception {
        _columnsToExplode = null;
    }
    
    private StructType generateSchemaAllColumns(Row exampleRow) {
        _elementsPerExpansion = new HashMap<Integer, Integer>();
        
        // Generate the Schema & find the columns to extract
        StructType structure = exampleRow.schema();
        StructField[] currFields = structure.fields(); 
        List<StructField> newFields = new ArrayList<StructField>();
        for (int i = 0; i < currFields.length; i++) {
            StructField currField = currFields[i];
            String fieldName = currField.name();
            DataType currDataType = currField.dataType();
            boolean currNullable = currField.nullable();
            newFields.add(DataTypes.createStructField(fieldName, currDataType, currNullable));
            if (currDataType.equals(new VectorUDT().defaultConcreteType())) {
                DenseVector vector = (DenseVector) exampleRow.get(i);
                int vectorSize = vector.size();
                _elementsPerExpansion.put(i, vectorSize);
                for (int j = 0; j < vectorSize; j++) {
                    String newFieldName = fieldName + "_" + j;
                    newFields.add(DataTypes.createStructField(newFieldName, DataTypes.DoubleType, true));
                }
            }
        }
        return DataTypes.createStructType(newFields);
    }
    
    public ExpressionEncoder<Row> getEncoder(Row row) {
        ExpressionEncoder<Row> encoder = RowEncoder.apply(generateSchemaAllColumns(row));
        return encoder;
    }
    
    public ExpressionEncoder<Row> getEncoder(StructType structure) {
        ExpressionEncoder<Row> encoder = RowEncoder.apply(generateSchemaSelectColumns(structure));
        return encoder;
    }
    
    public StructType getSchema(StructType structure) {
        return generateSchemaSelectColumns(structure);
    }

    @Override
    public Iterator<Row> call(Iterator<Row> rows) throws Exception {
        List<Row> outputRows = new ArrayList<Row>();
        outputRows = explodeAllColumns(rows);
        return outputRows.iterator();
    }
    
    private List<Row> explodeAllColumns(Iterator<Row> rows) {
        List<Row> outputRows = new ArrayList<Row>();
        while (rows.hasNext()) {
            Row currRow = rows.next();
            List<Object> rowElements = new ArrayList<Object>();
            for (int i = 0; i < currRow.size(); i++) {
                rowElements.add(currRow.get(i));
                if (_elementsPerExpansion.containsKey(i)) {
                    DenseVector vector = (DenseVector) currRow.get(i);
                    int numElements = _elementsPerExpansion.get(i);
                    double[] array = vector.toArray();
                    for (int j = 0; j < numElements; j++) {
                        rowElements.add(array[j]);
                    }
                }
            }
            Row newRow = RowFactory.create(rowElements.toArray());
            outputRows.add(newRow);
        }
        return outputRows;
    }
}