package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.clustering.ClusteringModelInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.Comparator;
import java.util.List;

/**
 * KMeansModelInfoBatchOp can be linked to the output of KMeansTrainBatchOp to summary the kmeans model.
 */
public class KMeansModelInfoBatchOp extends ExtractModelInfoBatchOp<KMeansModelInfoBatchOp.KMeansModelInfo, KMeansModelInfoBatchOp> {
    public KMeansModelInfoBatchOp() {
        this(null);
    }

    public KMeansModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    public KMeansModelInfo createModelInfo(List<Row> rows) {
        return new KMeansModelInfo(
            KMeansUtil.transformPredictDataToTrainData(new KMeansModelDataConverter().load(rows)));
    }

    /**
     * Summary of KMeansModel.
     */
    public static class KMeansModelInfo extends ClusteringModelInfo {
        private KMeansTrainModelData modelData;
        private int totalSamples = 0;
        private int vectorSize;

        public KMeansModelInfo(KMeansTrainModelData modelData){
            this.modelData = modelData;
            modelData.centroids.sort(Comparator.comparing(o -> o.getClusterId()));
            for(KMeansTrainModelData.ClusterSummary summary : modelData.centroids){
                totalSamples += summary.getWeight();
            }
            vectorSize = modelData.params.vectorSize;
        }

        @Override
        public int getClusterNumber(){
            return modelData.centroids.size();
        }

        @Override
        public DenseVector getClusterCenter(long clusterId){
            return modelData.getClusterVector((int)clusterId);
        }

        public double getClusterWeight(long clusterId){
            return modelData.getClusterWeight((int)clusterId);
        }

        @Override
        public String toString(){
            return clusterCenterToString(20, "KMeans",
                " Clustering on " + totalSamples + " samples of " + vectorSize + " dimension based on " +
                    modelData.params.distanceType.toString() + ".");
        }
    }
}
