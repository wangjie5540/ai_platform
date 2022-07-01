package com.digitforce.algorithm.replenishment.component.grossDemand;


import com.digitforce.algorithm.replenishment.component.demand.OrderPoint;

public class NewGoodsOrderPointGD extends OrderPoint {
    String expression = "max(毛需求_起订点 + 1, 最小陈列量)";

    @Override
    public double postProcess(double value) {
        double orderPoint = value - 1;
        setVariable("毛需求", value);

        String equation = "毛需求 - 1";
        processReplLog(equation, "毛需求", orderPoint);
        return orderPoint;
    }

    public NewGoodsOrderPointGD() {
        setExpression(expression);
    }
}
