package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared_params.HasDataFormat;
import com.alibaba.alink.params.io.shared_params.HasTopic;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface KafkaSinkParams<T> extends WithParams<T>,
    HasTopic<T>, HasDataFormat<T>, HasFieldDelimiterDvComma<T>, HasProperties<T> {

    ParamInfo<String> BOOTSTRAP_SERVERS = ParamInfoFactory
        .createParamInfo("bootstrapServers", String.class)
        .setDescription("kafka bootstrap servers")
        .setRequired()
        .setAlias(new String[]{"bootstrap.servers"})
        .build();

    default String getBootstrapServers() {
        return get(BOOTSTRAP_SERVERS);
    }

    default T setBootstrapServers(String value) {
        return set(BOOTSTRAP_SERVERS, value);
    }
}
