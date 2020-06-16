package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.params.ParamUtil;

public interface HasTreeType<T> extends WithParams<T> {
	ParamInfo <TreeType> TREE_TYPE = ParamInfoFactory
		.createParamInfo("treeType", TreeType.class)
		.setDescription("The criteria of the tree. " +
			"There are three options: \"AVG\", \"partition\" or \"gini(infoGain, infoGainRatio)\""
		)
		.setHasDefaultValue(TreeType.AVG)
		.build();

	default TreeType getTreeType() {
		return get(TREE_TYPE);
	}

	default T setTreeType(TreeType value) {
		return set(TREE_TYPE, value);
	}

	default T setTreeType(String value) {
		return set(TREE_TYPE, ParamUtil.searchEnum(TREE_TYPE, value));
	}

	/**
	 * Indicate that the criteria using in the random forest.
	 */
	enum TreeType {
		/**
		 * Ave Partition the tree as hybrid.
		 */
		AVG,

		/**
		 * Partition the tree as hybrid.
		 */
		PARTITION,

		/**
		 * Gini index. ref: cart.
		 */
		GINI,

		/**
		 * Info gain. ref: id3
		 */
		INFOGAIN,

		/**
		 * Info gain ratio. ref: c4.5
		 */
		INFOGAINRATIO
	}
}
