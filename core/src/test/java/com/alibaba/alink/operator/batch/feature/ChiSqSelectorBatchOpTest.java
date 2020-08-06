package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.feature.ChisqSelectorModelInfo;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Consumer;

public class ChiSqSelectorBatchOpTest {

    @Test
    public void test() throws Exception {
        Row[] testArray =
            new Row[]{
                Row.of("a", 1L, 1, 2.0, true),
                Row.of(null, 2L, 2, -3.0, true),
                Row.of("c", null, null, 2.0, false),
                Row.of("a", 0L, 0, null, null),
            };

        String[] colNames = new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"};

        MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

        ChiSqSelectorBatchOp selector = new ChiSqSelectorBatchOp()
            .setSelectedCols(new String[]{"f_string", "f_long", "f_int", "f_double"})
            .setLabelCol("f_boolean")
            .setNumTopFeatures(2);

        selector.linkFrom(data);

        selector.lazyPrintModelInfo();

        selector.lazyCollectModelInfo(
            new Consumer<ChisqSelectorModelInfo>() {
                @Override
                public void accept(ChisqSelectorModelInfo chisqSelectorSummary) {
                    Assert.assertEquals(chisqSelectorSummary.chisq("f_long"), 8.0, 10e-10);
                    Assert.assertEquals(chisqSelectorSummary.chisq("f_int"), 8.0, 10e-10);
                    Assert.assertEquals(chisqSelectorSummary.chisq("f_string"), 5.0, 10e-10);
                    Assert.assertEquals(chisqSelectorSummary.chisq("f_double"), 5.0, 10e-10);
                    Assert.assertEquals(chisqSelectorSummary.pValue("f_double"), 0.2872974951836462, 10e-10);
                 }
            }
        );

        BatchOperator.execute();
    }

}