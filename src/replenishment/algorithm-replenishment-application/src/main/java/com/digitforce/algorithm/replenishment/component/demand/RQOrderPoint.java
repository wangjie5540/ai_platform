package com.digitforce.algorithm.replenishment.component.demand;

import com.digitforce.algorithm.replenishment.component.unitComponent.SafetyStockBound;

public class RQOrderPoint extends SafetyStockBound {
    String name = "毛需求_起订点";
    String expression = "max(最低库存, min(过去7天日均销, 未来7天预测日均销) * (安全库存限制系数 + 展望期天数))";

    @Override
    public void setDefaultVariables() {
        super.setDefaultVariables();
        setVariable("最低库存", 0);
    }

    public RQOrderPoint() {
        setName(name);
        setExpression(expression);
    }
}
