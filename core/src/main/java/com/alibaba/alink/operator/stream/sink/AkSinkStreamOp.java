package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.AkSinkParams;

@IoOpAnnotation(name = "alink_file", ioType = IOType.SinkStream)
public final class AkSinkStreamOp extends BaseSinkStreamOp<AkSinkStreamOp>
	implements AkSinkParams<AkSinkStreamOp> {

	public AkSinkStreamOp() {
		this(new Params());
	}

	public AkSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSinkStreamOp.class), params);
	}

	@Override
	public AkSinkStreamOp sinkFrom(StreamOperator in) {
		in.getDataStream()
			.addSink(
				new OutputFormatSinkFunction<>(
					new AkUtils.AkOutputFormat(
						getFilePath(),
						new AkUtils.AkMeta(CsvUtil.schema2SchemaStr(in.getSchema())),
						getOverwriteSink() ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE)
				)
			)
			.setParallelism(getNumFiles())
			.name("AkSink");

		return this;
	}
}
