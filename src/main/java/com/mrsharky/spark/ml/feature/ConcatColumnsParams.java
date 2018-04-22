/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrsharky.spark.ml.feature;

import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.Params;
import org.apache.spark.ml.param.StringArrayParam;

/**
 *
 * @author mrsharky
 */
public interface ConcatColumnsParams extends Params {

    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam);
    public void org$apache$spark$ml$param$shared$HasOutputCol$_setter_$outputCol_$eq(Param param);
    public void org$apache$spark$ml$param$shared$HasConcatValue$_setter_$concatValue_$eq(Param param);
    
    public StringArrayParam inputCols();
    public Param<String> outputCol();
    public Param<String> concatValue();
    
    public String[] getInputCols();
    public String getOutputCol();
    public String getConcatValue();
}
