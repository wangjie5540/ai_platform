//package com.digitforce.algorithm.replenishment.component.demand;
//
//import org.apache.commons.math3.distribution.AbstractRealDistribution;
//import org.apache.commons.math3.distribution.TDistribution;
//
//public class RegularGoodsOrderPoint extends OrderPoint{
//    String name = "起订点";
////    String expression = "max(最低库存, 展望期销量预测 + T值 * sqrt(展望期天数方差) - 1)";
//    String expression = "max(最低库存, 展望期销量预测 + 安全库存 - 1)";
//
//    public void setDefaultVariables() {
//        setVariable("最低库存", 0);
//        setVariable("展望期销量预测", 0);
//        setVariable("展望期天数方差", 0);
//        setVariable("样本数", 0);
//        setVariable("服务水平", 0);
//    }
//
//    public RegularGoodsOrderPoint() {
//        super();
//        setName(name);
//        setExpression(expression);
//
//    }
//
//    @Override
//    public void preProcess() {
//        double dof = getVariable("样本数");
//        double sl = getVariable("服务水平");
//        double p = 0.5 + sl/2.0;
//
//        AbstractRealDistribution dist = new TDistribution(dof);
//        double tvalue = dist.inverseCumulativeProbability(p);
//        setVariable("T值", tvalue);
//    }
//
//
//    @Override
//    public double postProcess(double value) {
//        return Math.ceil(value);
//    }
//}
