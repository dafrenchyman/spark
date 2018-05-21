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

import com.mrsharky.spark.functions.UniqueCountsCombiner;
import com.mrsharky.spark.functions.UniqueCountsCreateCombinerWithTotal;
import com.mrsharky.spark.functions.ReMapJavaPairRddRowTogether;
import com.mrsharky.spark.functions.FlatmapToPairWithCounts;
import java.io.Serializable;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

/**
 *
 * @author jpierret
 */
public class JavaRddUtils implements Serializable {
        
    /**
     * Function that takes in a dataset and counts the distinct and total counts for the keys submitted
     * @param data Input dataset
     * @param keys Keys we want to pair on. The rest of the data will be dropped
     * @return 
     */
    public static JavaRDD<Row> ReduceCountsByKeys(JavaRDD<Row> data, List<Integer> keys, int totalColumnLocation) {

        // Convert the data to pairs. Where: Row1 = keys, Row2 = totalCount, uniqueCount
        FlatmapToPairWithCounts fph = new FlatmapToPairWithCounts(keys, totalColumnLocation);
        JavaPairRDD<Row, Row> pairedMap = data.flatMapToPair(c -> fph.call(c));

        // Combine By Key (reduces on each partition, then combines the different partitions with the same key, then combines again)
        UniqueCountsCombiner ucr = new UniqueCountsCombiner();
        UniqueCountsCreateCombinerWithTotal createComb = new UniqueCountsCreateCombinerWithTotal(0);
        JavaPairRDD<Row, Row> groupCombiner = pairedMap.combineByKey(createComb, ucr, ucr);

        // Remove the grouping
        ReMapJavaPairRddRowTogether combinePartitioner = new ReMapJavaPairRddRowTogether();
        JavaRDD<Row> results = groupCombiner.mapPartitions(c -> combinePartitioner.call(c));
        
        return results;
    }
}
