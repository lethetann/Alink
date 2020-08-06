package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.io.AkSourceParams;

@IoOpAnnotation(name = "ak", ioType = IOType.SourceBatch)
public final class AkSourceBatchOp extends BaseSourceBatchOp<AkSourceBatchOp>
	implements AkSourceParams<AkSourceBatchOp> {

	public AkSourceBatchOp() {
		this(new Params());
	}

	public AkSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		final AkUtils.AkMeta meta = AkUtils.getMetaFromPath(getFilePath());

		return DataSetConversionUtil.toTable(
			getMLEnvironmentId(),
			MLEnvironmentFactory
				.get(getMLEnvironmentId())
				.getExecutionEnvironment()
				.createInput(new AkUtils.AkInputFormat(getFilePath(), meta))
				.name("AkSource")
				.rebalance(),
			CsvUtil.schemaStr2Schema(meta.schemaStr)
		);
	}

}
