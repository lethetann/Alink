package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.params.recommendation.FmPredictParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Fm predict batch operator. this operator predict data's label with fm model.
 *
 */
public final class FmRegressorPredictBatchOp extends ModelMapBatchOp <FmRegressorPredictBatchOp>
	implements FmPredictParams<FmRegressorPredictBatchOp> {

	private static final long serialVersionUID = -1174383656892662407L;

	public FmRegressorPredictBatchOp() {
		this(new Params());
	}

	public FmRegressorPredictBatchOp(Params params) {
		super(FmModelMapper::new, params);
	}

}
