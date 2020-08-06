package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit test for QuantileDiscretizerModelMapper.
 */
public class QuantileDiscretizerModelMapperTest {
    static Row[] rows = new Row[]{
        Row.of(0L, "{\"selectedCols\":\"[\\\"col2\\\",\\\"col3\\\"]\",\"version\":\"\\\"v2\\\"\","
            + "\"numBuckets\":\"3\"}\n"),
        Row.of(1048576L, "[{\"binDivideType\":\"QUANTILE\",\"featureName\":\"col2\",\"bin\":{\"NORM\":[{\"index\":0},"
            + "{\"index\":1},{\"index\":2}],\"NULL\":{\"index\":3}},\"featureType\":\"INT\",\"splitsArray\":[-2,1],"
            + "\"isLeftOpen\":true,\"binCount\":3}]\n"),
        Row.of(2097152L, "[{\"binDivideType\":\"QUANTILE\",\"featureName\":\"col3\",\"bin\":{\"NORM\":[{\"index\":0},"
            + "{\"index\":1},{\"index\":2}],\"NULL\":{\"index\":3}},\"featureType\":\"DOUBLE\",\"splitsArray\":[0.9,"
            + "1.1],\"isLeftOpen\":true,\"binCount\":3}]\n")
    };
    static List<Row> model = Arrays.asList(rows);

    private static TableSchema modelSchema = new QuantileDiscretizerModelDataConverter().getModelSchema();
    private static TableSchema dataSchema = new TableSchema(
        new String[] {"col1", "col2", "col3"},
        new TypeInformation<?>[] {Types.STRING, Types.LONG, Types.DOUBLE}
    );

    private static Row defaultRow = Row.of("a", -3L, 1.5);
    private static Row nullElseRow = Row.of("b", null, 1.1);

    @Test
    public void testAssembledVector() throws Exception {
        Params params = new Params()
            .set(HasOutputCols.OUTPUT_COLS, new String[]{"pred"})
            .set(QuantileDiscretizerPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.ASSEMBLED_VECTOR)
            .set(QuantileDiscretizerPredictParams.SELECTED_COLS, new String[]{"col2", "col3"})
            .set(QuantileDiscretizerPredictParams.RESERVED_COLS, new String[] {})
            .set(QuantileDiscretizerPredictParams.DROP_LAST, false);

        QuantileDiscretizerModelMapper mapper = new QuantileDiscretizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(defaultRow).getField(0).toString(), "$8$0:1.0 6:1.0");
        assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"pred"},
            new TypeInformation <?>[] {VectorTypes.SPARSE_VECTOR}));
    }

    @Test
    public void testReserved() throws Exception {
        Params params = new Params()
            .set(HasOutputCols.OUTPUT_COLS, new String[]{"pred"})
            .set(QuantileDiscretizerPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.ASSEMBLED_VECTOR)
            .set(QuantileDiscretizerPredictParams.SELECTED_COLS, new String[]{"col2", "col3"})
            .set(QuantileDiscretizerPredictParams.DROP_LAST, false);

        QuantileDiscretizerModelMapper mapper = new QuantileDiscretizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(defaultRow).getField(3).toString(), "$8$0:1.0 6:1.0");
        assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"col1", "col2", "col3", "pred"},
            new TypeInformation <?>[] {Types.STRING, Types.LONG, Types.DOUBLE, VectorTypes.SPARSE_VECTOR}));
    }

    @Test
    public void testIndex() throws Exception {
        Params params = new Params()
            .set(QuantileDiscretizerPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.INDEX)
            .set(QuantileDiscretizerPredictParams.SELECTED_COLS, new String[]{"col2", "col3"});

        QuantileDiscretizerModelMapper mapper = new QuantileDiscretizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);
        assertEquals(mapper.map(defaultRow), Row.of("a", 0L, 2L));
        assertEquals(mapper.map(nullElseRow), Row.of("b", 3L, 1L));
    }

    @Test
    public void testVector() throws Exception {
        Params params = new Params()
            .set(QuantileDiscretizerPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.VECTOR)
            .set(QuantileDiscretizerPredictParams.SELECTED_COLS, new String[]{"col2", "col3"})
            .set(QuantileDiscretizerPredictParams.DROP_LAST, false);

        QuantileDiscretizerModelMapper mapper = new QuantileDiscretizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);
        assertEquals(mapper.map(defaultRow), Row.of("a", new SparseVector(4, new int[]{0}, new double[]{1.0}),
            new SparseVector(4, new int[]{2}, new double[]{1.0})));
        assertEquals(mapper.map(nullElseRow), Row.of("b", new SparseVector(4, new int[]{3}, new double[]{1.0}),
            new SparseVector(4, new int[]{1}, new double[]{1.0})));
    }

    @Test
    public void testDropLast() throws Exception {
        Params params = new Params()
            .set(QuantileDiscretizerPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.VECTOR)
            .set(QuantileDiscretizerPredictParams.SELECTED_COLS, new String[]{"col2", "col3"})
            .set(QuantileDiscretizerPredictParams.DROP_LAST, true);

        QuantileDiscretizerModelMapper mapper = new QuantileDiscretizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(defaultRow), Row.of("a", new SparseVector(3, new int[]{0}, new double[]{1.0}),
            new SparseVector(3)));
        assertEquals(mapper.map(nullElseRow), Row.of("b", new SparseVector(3, new int[]{2}, new double[]{1.0}),
            new SparseVector(3, new int[]{1}, new double[]{1.0})));
    }

}