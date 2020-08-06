package com.alibaba.alink.pipeline;

import java.lang.reflect.ParameterizedType;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTransformInfo;
import com.alibaba.alink.common.lazy.LazyObjectsManager;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.lazy.WithTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import static com.alibaba.alink.common.lazy.HasLazyPrintModelInfo.LAZY_PRINT_MODEL_INFO_ENABLED;
import static com.alibaba.alink.common.lazy.HasLazyPrintModelInfo.LAZY_PRINT_MODEL_INFO_TITLE;
import static com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo.LAZY_PRINT_TRAIN_INFO_ENABLED;
import static com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo.LAZY_PRINT_TRAIN_INFO_TITLE;

/**
 * Abstract class for a trainer that train a machine learning model.
 *
 * The different between {@link EstimatorBase} and {@link Trainer} is that
 * some of {@link EstimatorBase} have its own feature such as some ensemble algorithms,
 * some frequent item set mining algorithms, auto tuning, etc.
 *
 * @param <T> The class type of the {@link Trainer} implementation itself
 * @param <M> class type of the {@link ModelBase} this Trainer produces.
 */
public abstract class Trainer<T extends Trainer <T, M>, M extends ModelBase<M>>
	extends EstimatorBase<T, M> implements HasLazyPrintTransformInfo<T> {

	public Trainer() {
		super();
	}

	public Trainer(Params params) {
		super(params);
	}

	@Override
	public M fit(BatchOperator input) {
		BatchOperator<?> trainer = postProcessTrainOp(train(input));
		return postProcessModel(createModel(trainer.getOutputTable()));
	}

	protected BatchOperator<?> postProcessTrainOp(BatchOperator<?> trainOp) {
		LazyObjectsManager lazyObjectsManager = MLEnvironmentFactory.get(trainOp.getMLEnvironmentId())
			.getLazyObjectsManager();
		lazyObjectsManager.genLazyTrainOp(this).addValue(trainOp);
		if (this instanceof HasLazyPrintTrainInfo) {
			if (get(LAZY_PRINT_TRAIN_INFO_ENABLED)) {
				((WithTrainInfo<?, ?>)trainOp).lazyPrintTrainInfo(get(LAZY_PRINT_TRAIN_INFO_TITLE));
			}
		}
		if (this instanceof HasLazyPrintModelInfo) {
			if (get(LAZY_PRINT_MODEL_INFO_ENABLED)) {
				((WithModelInfoBatchOp<?, ?, ?>)trainOp).lazyPrintModelInfo(get(LAZY_PRINT_MODEL_INFO_TITLE));
			}
		}
		return trainOp;
	}

	protected M postProcessModel(M model) {
		LazyObjectsManager lazyObjectsManager = MLEnvironmentFactory.get(model.getMLEnvironmentId())
			.getLazyObjectsManager();
		lazyObjectsManager.genLazyModel(this).addValue(model);
		if (this instanceof HasLazyPrintTransformInfo) {
			if (get(LAZY_PRINT_TRANSFORM_DATA_ENABLED)) {
				model.enableLazyPrintTransformData(
					get(LAZY_PRINT_TRANSFORM_DATA_NUM),
					get(LAZY_PRINT_TRANSFORM_DATA_TITLE));
			}
			if (get(LAZY_PRINT_TRANSFORM_STAT_ENABLED)) {
				model.enableLazyPrintTransformStat(get(LAZY_PRINT_TRANSFORM_STAT_TITLE));
			}
		}
		return model;
	}

	@Override
	public M fit(StreamOperator input) {
		throw new UnsupportedOperationException("Only support batch fit!");
	}

	private M createModel(Table model) {
		try {
			ParameterizedType pt =
				(ParameterizedType) this.getClass().getGenericSuperclass();

			Class <M> classM = (Class <M>) pt.getActualTypeArguments()[1];

			return (M) classM.getConstructor(Params.class)
				.newInstance(getParams())
				.setModelData(model);

		} catch (Exception ex) {
			throw new RuntimeException(ex.toString());
		}
	}

	protected abstract BatchOperator train(BatchOperator in);

	protected StreamOperator train(StreamOperator in) {
		throw new UnsupportedOperationException("Only support batch fit!");
	}

}
