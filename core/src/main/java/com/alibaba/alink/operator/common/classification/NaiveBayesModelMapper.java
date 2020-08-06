package com.alibaba.alink.operator.common.classification;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelMapper;
import com.alibaba.alink.operator.common.dataproc.NumericalTypeCastMapper;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This mapper predicts sample label.
 */
public class NaiveBayesModelMapper extends RichModelMapper {
    private String[] colNames;
    private int[] featureIndices;
    private NaiveBayesModelData modelData;
    private MultiStringIndexerModelMapper stringIndexerModelPredictor;
    private NumericalTypeCastMapper stringIndexerModelNumericalTypeCastMapper;
    protected NumericalTypeCastMapper numericalTypeCastMapper;
    private boolean getCate;
    private final double constant = 0.5 * Math.log(2 * Math.PI);
    private final double maxValue = Math.log(Double.MAX_VALUE);
    private final double minValue = Math.log(Double.MIN_NORMAL);

    public NaiveBayesModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        this.colNames = dataSchema.getFieldNames();
    }

    @Override
    protected Object predictResult(Row row) throws Exception {
        double[] probs = calculateProb(row);
        return NaiveBayesTextModelMapper.findMaxProbLabel(probs, modelData.label);
    }

    @Override
    protected Tuple2<Object, String> predictResultDetail(Row row) throws Exception {
        double[] probs = calculateProb(row);
        Object label = NaiveBayesTextModelMapper.findMaxProbLabel(probs, modelData.label);
        String jsonDetail = NaiveBayesTextModelMapper.generateDetail(probs, modelData.piArray, modelData.label);
        return Tuple2.of(label, jsonDetail);
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        this.modelData = new NaiveBayesModelDataConverter().load(modelRows);
        int featureNumber = modelData.featureNames.length;
        featureIndices = new int[featureNumber];
        for (int i = 0; i < featureNumber; ++i) {
            featureIndices[i] = TableUtil.findColIndex(colNames, modelData.featureNames[i]);
        }
        TableSchema modelSchema = getModelSchema();
        List<String> listCateCols = new ArrayList<>();
        for (int i = 0; i < featureNumber; i++) {
            if (modelData.isCate[i]) {
                listCateCols.add(modelData.featureNames[i]);
            }
        }
        String[] categoricalColNames = listCateCols.toArray(new String[0]);
        getCate = categoricalColNames.length != 0;
        if (getCate) {
            stringIndexerModelPredictor = new MultiStringIndexerModelMapper(
                modelSchema,
                getDataSchema(),
                new Params()
                    .set(HasSelectedCols.SELECTED_COLS, categoricalColNames)
                    .set(HasHandleInvalid.HANDLE_INVALID, HasHandleInvalid.HandleInvalid.SKIP)
            );
            stringIndexerModelPredictor.loadModel(modelData.stringIndexerModelSerialized);

            stringIndexerModelNumericalTypeCastMapper = new NumericalTypeCastMapper(getDataSchema(),
                new Params()
                    .set(NumericalTypeCastParams.SELECTED_COLS, categoricalColNames)
                    .set(NumericalTypeCastParams.TARGET_TYPE, NumericalTypeCastParams.TargetType.valueOf("INT"))
            );
        }

        numericalTypeCastMapper = new NumericalTypeCastMapper(getDataSchema(),
            new Params()
                .set(
                    NumericalTypeCastParams.SELECTED_COLS,
                    ArrayUtils.removeElements(modelData.featureNames, categoricalColNames)//to check col Names.
                )
                .set(NumericalTypeCastParams.TARGET_TYPE, NumericalTypeCastParams.TargetType.valueOf("DOUBLE"))
        );
    }

    private Row transRow(Row row) throws Exception {
        if (getCate) {
            row = stringIndexerModelPredictor.map(row);
            row = stringIndexerModelNumericalTypeCastMapper.map(row);
        }

        return numericalTypeCastMapper.map(row);
    }

    /**
     * Calculate probability of the input data.
     */
    private double[] calculateProb(Row row) throws Exception {
        Row rowData = Row.copy(row);
        rowData = transRow(rowData);
        int labelSize = this.modelData.label.length;
        double[] probs = new double[labelSize];
        int featureSize = modelData.featureNames.length;
        int[] featureIndices = new int[featureSize];
        Arrays.fill(featureIndices, -1);
        boolean allZero = true;
        for (int i = 0; i < featureSize; i++) {
            if (rowData.getField(this.featureIndices[i]) != null) {
                featureIndices[i] = this.featureIndices[i];
                allZero = false;
            }
        }
        if (allZero) {
            double prob = 1. / labelSize;
            Arrays.fill(probs, prob);
            return probs;
        }
        for (int i = 0; i < labelSize; i++) {
            Number[][] labelData = modelData.theta[i];
            for (int j = 0; j < featureSize; j++) {
                int featureIndex = featureIndices[j];
                if (modelData.isCate[j]) {
                    if (featureIndex != -1) {
                        int index = (int) rowData.getField(featureIndex);
                        if (index < labelData[j].length) {
                            probs[i] += (Double) labelData[j][index];
                        }
                    }
                } else {
                    double miu = (double) labelData[j][0];
                    double sigma2 = (double) labelData[j][1];
                    if (featureIndex == -1) {
                        probs[i] -= (constant + 0.5 * Math.log(sigma2));
                    } else {
                        double data = (double) rowData.getField(featureIndex);
                        if (sigma2 == 0) {
                            if (Math.abs(data - miu) <= 1e-5) {
                                probs[i] += maxValue;
                            } else {
                                probs[i] += minValue;
                            }
                        } else {
                            double item1 = Math.pow(data - miu, 2) / (2 * sigma2);
                            probs[i] -= (item1 + constant + 0.5 * Math.log(sigma2));
                        }
                    }
                }

            }
        }
        BLAS.axpy(1, modelData.piArray, probs);
        return probs;
    }
}
