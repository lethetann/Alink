package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.operator.common.utils.PrettyDisplayUtils.displayList;

public class MaxAbsScalarModelInfo {
    double[] maxAbs;
    public MaxAbsScalarModelInfo(List<Row> rows) {
        double[] maxAbs = new MaxAbsScalerModelDataConverter().load(rows);
        this.maxAbs = maxAbs;
    }


    public double[] getMaxAbs() {
        return maxAbs;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append(PrettyDisplayUtils.displayHeadline("MaxAbsScalarModelInfo", '-')+"\n");
        res.append(PrettyDisplayUtils.displayHeadline("max abs information", '=')+"\n");
        res.append(displayList(Arrays.asList(ArrayUtils.toObject(maxAbs)), false) + "\n");
        return res.toString();
    }
}
