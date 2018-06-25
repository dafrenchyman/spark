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
package com.mrsharky.utilites;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * Code from: https://stackoverflow.com/questions/6701948/efficient-way-to-compare-version-strings-in-java
 */
public class VersionHelperTest {

    @Test
    public void testCompare() throws Exception {
        assertEquals(1, VersionHelper.compare("1", "0.9"));
        assertEquals(1, VersionHelper.compare("0.0.0.2", "0.0.0.1"));
        assertEquals(1, VersionHelper.compare("1.0", "0.9"));
        assertEquals(1, VersionHelper.compare("2.0.1", "2.0.0"));
        assertEquals(1, VersionHelper.compare("2.0.1", "2.0"));
        assertEquals(1, VersionHelper.compare("2.0.1", "2"));
        assertEquals(1, VersionHelper.compare("0.9.1", "0.9.0"));
        assertEquals(1, VersionHelper.compare("0.9.2", "0.9.1"));
        assertEquals(1, VersionHelper.compare("0.9.11", "0.9.2"));
        assertEquals(1, VersionHelper.compare("0.9.12", "0.9.11"));
        assertEquals(1, VersionHelper.compare("0.10", "0.9"));
        assertEquals(0, VersionHelper.compare("0.10", "0.10"));
        assertEquals(-1, VersionHelper.compare("2.10", "2.10.1"));
        assertEquals(-1, VersionHelper.compare("0.0.0.2", "0.1"));
        assertEquals(1, VersionHelper.compare("1.0", "0.9.2"));
        assertEquals(1, VersionHelper.compare("1.10", "1.6"));
        assertEquals(0, VersionHelper.compare("1.10", "1.10.0.0.0.0"));
        assertEquals(1, VersionHelper.compare("1.10.0.0.0.1", "1.10"));
        assertEquals(0, VersionHelper.compare("1.10.0.0.0.0", "1.10"));
        assertEquals(1, VersionHelper.compare("1.10.0.0.0.1", "1.10"));
    }
    
}
