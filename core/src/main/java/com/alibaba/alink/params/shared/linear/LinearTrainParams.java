package com.alibaba.alink.params.shared.linear;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * parameters of linear training.
 */
public interface LinearTrainParams<T> extends
    HasWithIntercept<T>,
    HasMaxIterDefaultAs100<T>,
    HasEpsilonDv0000001<T>,
    HasFeatureColsDefaultAsNull<T>,
    HasLabelCol<T>,
    HasWeightColDefaultAsNull<T>,
    HasVectorColDefaultAsNull<T>,
    HasStandardization<T> {

    ParamInfo<OptimMethod> OPTIM_METHOD = ParamInfoFactory
        .createParamInfo("optimMethod", OptimMethod.class)
        .setDescription("optimization method")
        .setHasDefaultValue(null)
        .build();

    default OptimMethod getOptimMethod() {
        return get(OPTIM_METHOD);
    }

    default T setOptimMethod(String value) {
        return set(OPTIM_METHOD, ParamUtil.searchEnum(OPTIM_METHOD, value));
    }

    default T setOptimMethod(OptimMethod value) {
        return set(OPTIM_METHOD, value);
    }

    /**
     * Optimization Type.
     */
    enum OptimMethod {
        /**
         * LBFGS method
         */
        LBFGS,
        /**
         * Gradient Descent method
         */
        GD,
        /**
         * Newton method
         */
        Newton,
        /**
         * Stochastic Gradient Descent method
         */
        SGD,
        /**
         * OWLQN method
         */
        OWLQN
    }
}
