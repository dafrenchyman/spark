/*
 * MIT License
 *
 * Copyright (c) 2018 Julien Pierret
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.mrsharky.spark.ml.feature;

import ml.dmlc.xgboost4j.scala.EvalTrait;
import ml.dmlc.xgboost4j.scala.ObjectiveTrait;
import ml.dmlc.xgboost4j.scala.spark.XGBoostModel;
import org.apache.spark.sql.Dataset;
import scala.collection.immutable.Map;

/**
 *
 * @author mrsharky
 */
public class XGBoostEstimator extends ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator {
        
    /**
     * Booster to use, options: {"gbtree", "gblinear", "dart"}
     * @param boosterType
     * @return
     * @throws Exception 
     */
    public XGBoostEstimator setBoosterType(String boosterType) throws Exception {
        switch (boosterType) {
            case "gbtree" :
                break;
            case "gblinear" :
                break;
            case "dart" :
                break;
            default:
                throw new Exception("Invalid booster type: " + boosterType + " (Allowed types: gbtree, gblinear, dart");
        }
        this.set(this.boosterType(), boosterType);
        return this;
    }
    
    /**
     * Step size shrinkage used in update to prevents overfitting. After each boosting step, we
     * can directly get the weights of new features and eta actually shrinks the feature weights
     * to make the boosting process more conservative. [default=0.3] range: [0,1]
     * @param eta
     * @return 
     */
    public XGBoostEstimator setEta(double eta) {
        this.set(this.eta(), eta);
        return this;
    }
    
    /**
     * minimum loss reduction required to make a further partition on a leaf node of the tree.
     * the larger, the more conservative the algorithm will be. [default=0] range: [0,
     * Double.MaxValue]
     * @param gamma
     * @return 
     */
    public XGBoostEstimator setGamma(double gamma) {
        this.set(this.gamma(), gamma);
        return this;
    }
    
    /**
     * maximum depth of a tree, increase this value will make model more complex / likely to be
     * overfitting. [default=6] range: [1, Int.MaxValue]
     * @param maxDepth
     * @return 
     */
    public XGBoostEstimator setMaxDepth(int maxDepth) {
        this.set(this.maxDepth(), maxDepth);
        return this;
    }
    
    /**
     * minimum sum of instance weight(hessian) needed in a child. If the tree partition step results
     * in a leaf node with the sum of instance weight less than min_child_weight, then the building
     * process will give up further partitioning. In linear regression mode, this simply corresponds
     * to minimum number of instances needed to be in each node. The larger, the more conservative
     * the algorithm will be. [default=1] range: [0, Double.MaxValue]
     * @param minChildWeight
     * @return 
     */
    public XGBoostEstimator setMinChildWeight(double minChildWeight) {
        this.set(this.minChildWeight(), minChildWeight);
        return this;
    }
    
    /**
     * Maximum delta step we allow each tree's weight estimation to be. If the value is set to 0, it
     * means there is no constraint. If it is set to a positive value, it can help making the update
     * step more conservative. Usually this parameter is not needed, but it might help in logistic
     * regression when class is extremely imbalanced. Set it to value of 1-10 might help control the
     * update. [default=0] range: [0, Double.MaxValue]
     * @param maxDeltaStep
     * @return 
     */
    public XGBoostEstimator setMaxDeltaStep(double maxDeltaStep) {
        this.set(this.maxDeltaStep(), maxDeltaStep);
        return this;
    }
    
    /**
     * subsample ratio of the training instance. Setting it to 0.5 means that XGBoost randomly
     * collected half of the data instances to grow trees and this will prevent overfitting.
     * [default=1] range:(0,1]
     * @param subSample
     * @return 
     */
    public XGBoostEstimator setSubSample(double subSample) {
        this.set(this.subSample(), subSample);
        return this;
    }
    
    /**
     * subsample ratio of columns when constructing each tree. [default=1] range: (0,1]
     * @param colSampleByTree
     * @return 
     */
    public XGBoostEstimator setColSampleByTree(double colSampleByTree) {
        this.set(this.colSampleByTree(), colSampleByTree);
        return this;
    }
    
    /**
     * subsample ratio of columns for each split, in each level. [default=1] range: (0,1]
     * @param colSampleByLevel
     * @return 
     */
    public XGBoostEstimator setColSampleByLevel(double colSampleByLevel) {
        this.set(this.colSampleByLevel(), colSampleByLevel);
        return this;
    }
    
    /**
     * L2 regularization term on weights, increase this value will make model more conservative.
     * [default=1]
     * @param lambda
     * @return 
     */
    public XGBoostEstimator setLambda(double lambda) {
        this.set(this.lambda(), lambda);
        return this;
    }
    
    /**
     * L1 regularization term on weights, increase this value will make model more conservative.
     * [default=0]
     * @param alpha
     * @return 
     */
    public XGBoostEstimator setAlpha(double alpha) {
        this.set(this.alpha(), alpha);
        return this;
    }
    
    /**
     * The tree construction algorithm used in XGBoost. options: {'auto', 'exact', 'approx'}
     * [default='auto']
     * @param treeMethod
     * @return 
     */
    public XGBoostEstimator setTreeMethod(String treeMethod) {
        this.set(this.treeMethod(), treeMethod);
        return this;
    }
    
    /**
     * growth policy for fast histogram algorithm
     * @param growthPolicy
     * @return 
     */
    public XGBoostEstimator setGrowthPolicy(String growthPolicy) {
        this.set(this.growthPolicty(), growthPolicy);
        return this;
    }
    
    /**
     * maximum number of bins in histogram
     * @param maxBins
     * @return 
     */
    public XGBoostEstimator setMaxBins(int maxBins) {
        this.set(this.maxBins(), maxBins);
        return this;
    }
    
    /**
     * This is only used for approximate greedy algorithm.
     * This roughly translated into O(1 / sketch_eps) number of bins. Compared to directly select
     * number of bins, this comes with theoretical guarantee with sketch accuracy.
     * [default=0.03] range: (0, 1)
     * @param sketchEps
     * @return 
     */
    public XGBoostEstimator setSketchEps(double sketchEps) {
        this.set(this.sketchEps(), sketchEps);
        return this;
    }
    
    /**
     * Control the balance of positive and negative weights, useful for unbalanced classes. A typical
     * value to consider: sum(negative cases) / sum(positive cases).   [default=0]
     * @param scalePosWeight
     * @return 
     */
    public XGBoostEstimator setScalePosWeight(double scalePosWeight) {
        this.set(this.scalePosWeight(), scalePosWeight);
        return this;
    }
     
    /**
     * Parameter for Dart booster.
     * Type of sampling algorithm. "uniform": dropped trees are selected uniformly.
     * "weighted": dropped trees are selected in proportion to weight. [default="uniform"]
     * @param sampleType
     * @return 
     */
    public XGBoostEstimator setSampleType(String sampleType) {
        this.set(this.sampleType(), sampleType);
        return this;
    }
    
    /**
     * Parameter of Dart booster.
     * type of normalization algorithm, options: {'tree', 'forest'}. [default="tree"]
     * @param normalizeType
     * @return 
     */
    public XGBoostEstimator setNormalizeType(String normalizeType) {
        this.set(this.normalizeType(), normalizeType);
        return this;
    }
    
    
    /**
     * Parameter of Dart booster.
     * dropout rate. [default=0.0] range: [0.0, 1.0]
     * @param rateDrop
     * @return 
     */
    public XGBoostEstimator setRateDrop(double rateDrop) {
        this.set(this.rateDrop(), rateDrop);
        return this;
    }
    
    /**
     * Parameter of Dart booster.
     * probability of skip dropout. If a dropout is skipped, new trees are added in the same manner
     * as gbtree. [default=0.0] range: [0.0, 1.0]
     * @param skipDrop
     * @return 
     */
    public XGBoostEstimator setSkipDrop(double skipDrop) {
        this.set(this.skipDrop(), skipDrop);
        return this;
    }
    
    /**
     * Parameter of linear booster
     * L2 regularization term on bias, default 0(no L1 reg on bias because it is not important)
     * @param lambdaBias
     * @return 
     */
    public XGBoostEstimator setLambdaBias(double lambdaBias) {
        this.set(this.lambdaBias(), lambdaBias);
        return this;
    }
    
    @Override
    public XGBoostEstimator setFeaturesCol(String value) {
        super.setFeaturesCol(value);
        return this;
    }
    
    @Override
    public XGBoostEstimator setLabelCol(String value) {
        super.setLabelCol(value);
        return this;
    }
    
    /**
     * The number of rounds for boosting
     * @param round
     * @return 
     */
    public XGBoostEstimator setRound(int round) {
        this.set(this.round(), round);
        return this;
    }
    
    /**
     * number of workers used to train xgboost model. default: 1
     * @param nWorkers
     * @return 
     */
    public XGBoostEstimator setNWorkers(int nWorkers) {
        this.set(this.nWorkers(), nWorkers);
        return this;
    }
    
    /**
     * number of threads used by per worker. default 1
     * @param numThreadPerTask
     * @return 
     */
    public XGBoostEstimator setNumThreadPerTask(int numThreadPerTask) {
        this.set(this.numThreadPerTask(), numThreadPerTask);
        return this;
    }
    
    /**
     * whether to use external memory as cache. default: false
     * @param useExternalMemory
     * @return 
     */
    public XGBoostEstimator setUseExternalMemory (boolean useExternalMemory) {
        this.set(this.useExternalMemory(), useExternalMemory);
        return this;
    }
    
    /**
     * 0 means printing running messages, 1 means silent mode. default: 0
     * @param silent
     * @return 
     */
    public XGBoostEstimator setSilent (int silent) {
        this.set(this.silent(), silent);
        return this;
    }
    
    /**
     * customized objective function provided by user. default: null
     * @param customObj
     * @return 
     */
    public XGBoostEstimator setCustomObj (ObjectiveTrait customObj) {
        this.set(this.customObj(), customObj);
        return this;
    }
    
    /**
     * Customized evaluation function provided by user. default: null
     * @param customEval
     * @return 
     */
    public XGBoostEstimator setCustomEval (EvalTrait customEval) {
        this.set(this.customEval(), customEval);
        return this;
    }
    
    /**
     * the value treated as missing. default: Float.NaN
     * @param missing
     * @return 
     */
    public XGBoostEstimator setMissing (float missing) {
        this.set(this.missing(), missing);
        return this;
    }
    
    /**
     * the maximum time to wait for the job requesting new workers. default: 30 minutes.
     * the maximum time to request new Workers if numCores are insufficient. 
     * The timeout will be disabled if this value is set smaller than or equal to 0.
     * @param timeoutRequestWorkers
     * @return 
     */
    public XGBoostEstimator setTimeoutRequestWorkers (long timeoutRequestWorkers) {
        this.set(this.timeoutRequestWorkers(), timeoutRequestWorkers);
        return this;
    }
    
    /**
     * The hdfs folder to load and save checkpoint boosters. default: `empty_string`
     * If there are existing checkpoints in checkpoint_path. The job will load
     * the checkpoint with highest version as the starting point for training. If
     * checkpoint_interval is also set, the job will save a checkpoint every a few rounds.
     * @param checkpointPath
     * @return 
     */
    public XGBoostEstimator setCheckpointPath (String checkpointPath) {
        this.set(this.checkpointPath(), checkpointPath);
        return this;
    }
    
    /**
     * Param for set checkpoint interval (&gt;= 1) or disable checkpoint (-1). E.g. 10 means that
     * the trained model will get checkpointed every 10 iterations. Note: `checkpoint_path` must
     * also be set if the checkpoint interval is greater than 0.
     * @param checkpointInterval
     * @return 
     */
    public XGBoostEstimator setCheckpointInterval (int checkpointInterval) {
        this.set(this.checkpointInterval(), checkpointInterval);
        return this;
    }
    
    public XGBoostEstimator setSeed (long seed) {
        this.set(this.seed(), seed);
        return this;
    }
    
    
    // Getters
    public String  getBoosterType()           { return                  this.get(this.boosterType()).get(); }
    public double  getEta()                   { return (double)         this.get(this.eta()).get(); }
    public double  getGamma()                 { return (double)         this.get(this.gamma()).get(); }
    public int     getMaxDepth()              { return (int)            this.get(this.maxDepth()).get(); }
    public double  getMinChildWeight()        { return (double)         this.get(this.minChildWeight()).get(); }
    public double  getMaxDeltaStep()          { return (double)         this.get(this.maxDeltaStep()).get(); }
    public double  getSubSample()             { return (double)         this.get(this.subSample()).get(); }
    public double  getColSampleByTree()       { return (double)         this.get(this.colSampleByTree()).get(); }
    public double  getColSampleByLevel()      { return (double)         this.get(this.colSampleByLevel()).get(); }
    public double  getLambda()                { return (double)         this.get(this.lambda()).get(); }
    public double  getAlpha()                 { return (double)         this.get(this.alpha()).get(); }
    public String  getTreeMethod()            { return                  this.get(this.treeMethod()).get(); }
    public String  getGrowthPolicy()          { return                  this.get(this.growthPolicty()).get(); }
    public int     getMaxBins()               { return (int)            this.get(this.maxBins()).get(); }
    public double  getSketchEps()             { return (double)         this.get(this.sketchEps()).get(); }
    public double  getScalePosWeight()        { return (double)         this.get(this.scalePosWeight()).get(); }
    public String  getSampleType()            { return                  this.get(this.sampleType()).get(); }
    public String  getNormalizeType()         { return                  this.get(this.normalizeType()).get(); }
    public double  getRateDrop()              { return (double)         this.get(this.rateDrop()).get(); }
    public double  getSkipDrop()              { return (double)         this.get(this.skipDrop()).get(); }
    public double  getLambdaBias()            { return (double)         this.get(this.lambdaBias()).get(); }
    public int     getRound()                 { return (int)            this.get(this.round()).get(); }
    public int     getNWorkers()              { return (int)            this.get(this.nWorkers()).get(); }
    public int     getNumThreadPerTask()      { return (int)            this.get(this.numThreadPerTask()).get(); }
    public ObjectiveTrait getCustomObj()      { return (ObjectiveTrait) this.get(this.customObj()).get(); }
    public EvalTrait getCustomEval()          { return (EvalTrait)      this.get(this.customEval()).get(); }
    public long    getTimeoutRequestWorkers() { return (long)           this.get(this.timeoutRequestWorkers()).get(); }  
    public String  getCheckpointPath()        { return                  this.get(this.checkpointPath()).get(); }
    public int     getCheckpointInterval()    { return (int)            this.get(this.checkpointInterval()).get(); }
    public long    getSeed()                  { return (long)           this.get(this.seed()).get(); }
      
    public XGBoostEstimator(String uid, Map<String, Object> xgboostParams) {
        super(uid, xgboostParams);
    }

    public XGBoostEstimator(Map<String, Object> xgboostParams) {
        super(xgboostParams);
    }

    public XGBoostEstimator(String uid) {
        super(uid);
    }
    
    public XGBoostModel fit(Dataset<?> dataset) {
        return super.fit(dataset);
    }

    

    
}
