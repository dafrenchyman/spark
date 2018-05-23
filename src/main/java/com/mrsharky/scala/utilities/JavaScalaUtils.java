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
package com.mrsharky.scala.utilities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import scala.collection.Seq;
import scala.Predef;
import scala.Tuple2;

/**
 *
 * @author Julien Pierret
 */
public class JavaScalaUtils {
    
    public static <T> Seq JavaListToScalaSeq(List<T> input) {
        Set<T> set = new HashSet<T>(input);
        return JavaConverters.asScalaIteratorConverter(set.iterator()).asScala().toSeq();
    }
    
    public static <T> scala.collection.Iterable JavaListToScalaIterable(List<T> input) {
        Set<T> set = new HashSet<T>(input);
        return JavaConverters.asScalaIteratorConverter(set.iterator()).asScala().toIterable();
    }
    
    public static <A, B> Map<A, B> JavaToImmutableScalaMap(HashMap<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
            Predef.<Tuple2<A, B>>conforms()
        );
    }
    
}
