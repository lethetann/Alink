package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LinearSvmTrainBatchOp;
import com.alibaba.alink.params.classification.LinearBinaryClassTrainParams;
import com.alibaba.alink.params.classification.LinearSvmPredictParams;
import com.alibaba.alink.pipeline.Trainer;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Linear svm pipeline op.
 *
 */
public class LinearSvm extends Trainer <LinearSvm, LinearSvmModel>
	implements LinearBinaryClassTrainParams<LinearSvm>, LinearSvmPredictParams <LinearSvm>,
	HasLazyPrintTrainInfo<LinearSvm>,
	HasLazyPrintModelInfo<LinearSvm> {

	private static final long serialVersionUID = 6241372711726213110L;

	public LinearSvm() {
		super();
	}

	public LinearSvm(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new LinearSvmTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
