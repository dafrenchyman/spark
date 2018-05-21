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

import com.mrsharky.spark.functions.Mapper_ReduceTotalCount;
import com.mrsharky.spark.ml.feature.models.TopCategoriesModel;
import com.mrsharky.spark.functions.PivotFieldsFromRowCount;
import com.mrsharky.spark.utilities.JavaRddUtils;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.param.IntArrayParam;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.util.MLReader;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.util.DefaultParamsWritable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.storage.StorageLevel;

/**
 *
 * @author Julien Pierret
 */
public class TopCategories extends Estimator<TopCategoriesModel> implements Serializable, DefaultParamsWritable {
    
    private StringArrayParam _inputCols;
    private IntArrayParam _numCategories;
    private String _uid;
    
   
    @Override
    public String uid() {
        return _uid;
    }
    
    public TopCategories(String uid) {
        _uid = uid;
    }

    public TopCategories() {
        _uid = TopCategories.class.getName() + "_" + UUID.randomUUID().toString();
    }
    
    public String[] getInputCols() {
        return this.get(_inputCols).get();
    }
    
    public int[] getNumCategories() {
        return this.get(_numCategories).get();
    }
    
    public TopCategories setInputCols(List<String> columns) {
        String[] columnsString = columns.toArray(new String[columns.size()]);
        return setInputCols(columnsString);
    }
    
    public TopCategories setInputCols(String[] names) {
        _inputCols = inputCols();
        this.set(_inputCols, names);
        return this;
    }
    
    public TopCategories setNumCategories(int[] catCount) {
        this._numCategories = numCategories();
        this.set(_numCategories, catCount);
        return this;
    }
    
    public TopCategories setNumCategories(int catCount) throws Exception {
        if (this.getInputCols() == null) {
            throw new Exception("Must first set the number of columns");
        }
        int numColumns = this.getInputCols().length;
        this._numCategories = numCategories();
        int numCats[] = new int[numColumns];
        for (int i = 0; i < numColumns; i++) {
            numCats[i] = catCount;
        }
        this.set(_numCategories, numCats);
        return this;
    }
    
    @Override
    public StructType transformSchema(StructType oldSchema) {
        return oldSchema;
    }
    
    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {
        this._inputCols = stringArrayParam;
    }
    
    public void org$apache$spark$ml$param$shared$HasNumCategories$_setter_$numCategories_$eq(IntArrayParam intArrayParam) {
        this._numCategories = intArrayParam;
    }
    
    @Override
    public TopCategoriesModel fit(Dataset<?> dataset) {
        StructType structType = dataset.schema();
        StructField[] fields = structType.fields();
        
        // Shrink all categorical Variables
        // We're assuming all String fields are going to be treated as categorical 
        // values. Since some of the categories have countless numbers of strings
        // We're going to make sure we only categories the top _n of them. The rest
        // Will be put into category "Other"
        // - String indexer doesn't work on NULL values (this is how we fix it)
        //      - https://issues.apache.org/jira/browse/SPARK-11569
        
        List<String> stringColumns = new ArrayList<String>();
        for (int counter = 0; counter < fields.length; counter++) {
            StructField currField = fields[counter];
            String currFieldName = currField.name();
            if (currField.dataType() == DataTypes.StringType) {                        
                stringColumns.add(currFieldName);
            }
        }
        SparkSession spark = dataset.sparkSession();
            
        // "Pivot" data to: variableName, variableValue, totalCount
        PivotFieldsFromRowCount pffr = new PivotFieldsFromRowCount(stringColumns, structType);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
        JavaRDD<Row> pivotedData = dataset.mapPartitions(f -> pffr.call((Iterator<Row>) f), encoder).javaRDD();
        
        // reduce pivoted data by a mapPartitioner (not perfect but should shrink the dataset some)
        int numCategories = Arrays.stream(this.getNumCategories()).max().getAsInt();
        Mapper_ReduceTotalCount mrtc = new Mapper_ReduceTotalCount(numCategories, 2);
        JavaRDD<Row> results = pivotedData.mapPartitions(s -> mrtc.call(s));
        
        results = JavaRddUtils.ReduceCountsByKeys(results, Arrays.asList(new Integer[]{0,1}), 2);
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("currVariable", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("currValue", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("TotalCount", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("UniqueCount", DataTypes.LongType, true));
        StructType schema = DataTypes.createStructType(structFields);
        Dataset<Row> allData = spark.createDataFrame(results, schema);
        allData = allData.persist(StorageLevel.DISK_ONLY());
        allData.createOrReplaceTempView("allData");
        
        // Try restricting
        Dataset<Row> subTable = spark.sql("\n"
                + " SELECT "
                + " currVariable \n"
                + " , currValue \n"
                + " , TotalCount \n"
                + " , row_number() OVER(PARTITION BY currVariable ORDER BY TotalCount DESC) AS transactionCounter \n"
                + " FROM allData");
        subTable.createOrReplaceTempView("SubTable");
        subTable.explain(true);
        List<Row> topElementsList = spark.sql("\n"
                + " SELECT \n"
                + " currVariable, currValue \n "
                + " FROM SubTable \n"
                + " WHERE transactionCounter <= " + numCategories + "\n")
                .collectAsList();
        
        // Restructure the List<Row> into the fields and values we will restrict
        Map<String, List<String>> elementRestrictions = new HashMap<String, List<String>>();
        for (int elementCounter = 0; elementCounter < topElementsList.size(); elementCounter++) {
            Row currRow = topElementsList.get(elementCounter);
            String currVariable = currRow.getString(0);
            String currValue = currRow.getString(1);            
            if (!elementRestrictions.containsKey(currVariable)) {
                elementRestrictions.put(currVariable, new ArrayList<String>());
            }
            elementRestrictions.get(currVariable).add(currValue);
        }
        TopCategoriesModel tcm = new TopCategoriesModel()
                .setLookupMap(elementRestrictions);
        return tcm;
    }

    @Override
    public Estimator<TopCategoriesModel> copy(ParamMap arg0) {
        TopCategories copied = new TopCategories()
                .setInputCols(this.getInputCols())
                .setNumCategories(this.getNumCategories());
        return copied;
    }

    @Override
    public MLWriter write() {
        return new TopCategoriesWriter(this);
    }

    @Override
    public void save(String path) throws IOException {
        write().saveImpl(path);
    }
    
    public static MLReader<TopCategories> read() {
        return new TopCategoriesReader();
    }
    
    public TopCategories load(String path) {
        return ((TopCategoriesReader)read()).load(path);
    }
    
    public StringArrayParam inputCols() {
        return new StringArrayParam(this, "inputCols", "Name of columns to be processed");
    }
    
    public IntArrayParam numCategories() {
        return new IntArrayParam(this, "numCategories", "Number of elements for each category");
    }
    
}
