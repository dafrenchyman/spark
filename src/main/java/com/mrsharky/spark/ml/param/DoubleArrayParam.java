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
package com.mrsharky.spark.ml.param;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.Params;
import org.apache.spark.ml.util.Identifiable;
import scala.Function1;

/**
 *
 * @author Julien Pierret
 */
public class DoubleArrayParam extends Param<Double[]> {
    
    private static Gson _gson = new GsonBuilder().create();

    @Override
    public String jsonEncode(Double[] value) {
        String json = _gson.toJson(value);
        return json;
    }

    @Override
    public Double[] jsonDecode(String json) {
        Type typeOfHashMap = new TypeToken<Double[]>() { }.getType();
        Double[] newMap = _gson.fromJson(json, typeOfHashMap);
        return newMap;
    }
    
    public DoubleArrayParam(Identifiable parent, String name, String doc) {
        super(parent, name, doc);
    }
    
    public DoubleArrayParam(String parent, String name, String doc) {
        super(parent, name, doc);
    }
    
    public DoubleArrayParam(Params parent, String name, String doc, Function1<Double[], Object> isValid) {
        super(parent, name, doc, isValid);
    }

    public DoubleArrayParam(Params parent, String name, String doc) {
        super(parent, name, doc);
    }
    
}
