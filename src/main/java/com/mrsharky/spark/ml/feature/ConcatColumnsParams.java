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

import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.Params;
import org.apache.spark.ml.param.StringArrayParam;

/**
 *
 * @author Julien Pierret
 */
public interface ConcatColumnsParams extends Params {

    public void com$mrsharky$spark$ml$feature$ConcatColumns$_setter_$inputCols_$eq(StringArrayParam stringArrayParam);
    public void com$mrsharky$spark$ml$feature$ConcatColumns$_setter_$outputCol_$eq(Param param);
    public void com$mrsharky$spark$ml$feature$ConcatColumns$_setter_$concatValue_$eq(Param param);
    
    public StringArrayParam inputCols();
    public Param<String> outputCol();
    public Param<String> concatValue();
    
    public String[] getInputCols();
    public String getOutputCol();
    public String getConcatValue();
}
