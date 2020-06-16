package com.alibaba.alink.operator.batch.utils;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UDFBatchOpTest {

    public static class LengthPlusValue extends ScalarFunction {
        public long eval(String s, long v) {
            return s.length() + v;
        }

        @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.LONG;
        }
    }

    @Test
    public void testDefaultReservedCols() throws Exception {
        MemSourceBatchOp src = new MemSourceBatchOp(new Object[][]{
            new Object[]{"1", "a", 1L},
            new Object[]{"2", "b33", 2L}
        }, new String[]{"c0", "c1", "c2"});

        UDFBatchOp udfOp = new UDFBatchOp()
            .setFunc(new LengthPlusValue())
            .setSelectedCols("c1", "c2")
            .setOutputCol("c2");

        udfOp.linkFrom(src);

        Assert.assertArrayEquals(new String[]{"c0", "c1", "c2"}, udfOp.getColNames());

        List<Row> expected = new ArrayList<>(Arrays.asList(
            Row.of("1", "a", 2L),
            Row.of("2", "b33", 5L)
        ));
        List<Row> results = udfOp.collect();
        Assert.assertEquals(expected, results);
    }

    @Test
    public void testEmptyReservedCols() throws Exception {
        MemSourceBatchOp src = new MemSourceBatchOp(new Object[][]{
            new Object[]{"1", "a", 1L},
            new Object[]{"2", "b33", 2L}
        }, new String[]{"c0", "c1", "c2"});

        UDFBatchOp udfOp = new UDFBatchOp()
            .setFunc(new LengthPlusValue())
            .setSelectedCols("c1", "c2")
            .setReservedCols(new String[]{})
            .setOutputCol("c2");

        udfOp.linkFrom(src);

        Assert.assertArrayEquals(new String[]{"c2"}, udfOp.getColNames());

        List<Row> expected = new ArrayList<>(Arrays.asList(
            Row.of(2L),
            Row.of(5L)
        ));
        List<Row> results = udfOp.collect();
        Assert.assertEquals(expected, results);
    }

}
