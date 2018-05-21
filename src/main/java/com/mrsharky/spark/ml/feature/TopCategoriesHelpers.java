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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;

/**
 *
 * @author Julien Pierret
 */
public class TopCategoriesHelpers {
    
    public static Map<String, List<String>> convertMapFromKeys(String[] keys, String[] values) {
        Map<String, List<String>> output = new HashMap<String, List<String>>();
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            String value = values[i];
            if (!output.containsKey(key)) {
                output.put(key, new ArrayList<String>());
            }
            output.get(key).add(value);
        }
        return output;
    }
    
    public static Map<String, List<Object>> convertToObject(Map<String, List<String>> input) {
        Map<String, List<Object>> output = new HashMap<String, List<Object>>();
        for (String key : input.keySet()) {
            output.put(key, new ArrayList<Object>());
            for (String value : input.get(key)) {
                output.get(key).add(value);
            }
        }
        return output;
    }
    
    public static Pair<String[], String[]> convertKeysFromMap(Map<String, List<String>> input) {
        // First calculate how big we need the arrays to be
        int totalInputs = 0;
        for (String key : input.keySet()) {
            totalInputs += input.get(key).size();
        }
        String[] keys   = new String[totalInputs];
        String[] values = new String[totalInputs];
        int counter = 0;
        for (String key : input.keySet()) {
            for (String value : input.get(key)) {
                keys[counter] = key;
                values[counter] = value;
                counter++;
            }
        }
        return Pair.with(keys, values);
    }
}
