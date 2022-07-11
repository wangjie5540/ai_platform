package com.digitforce.algorithm.replenishment.component.compoundComponent;


public class ReplPriorityReallocate extends ReplRatioReallocate {
    String name = "分配补货量";
    String expression = "min(补货量, 上级剩余库存)";

    @Override
    protected void setDefaultVariables() {
        super.setDefaultVariables();
        setVariable("补货量", 0.0);
        setVariable("父节点剩余库存", 0.0);
    }

    public ReplPriorityReallocate() {
        super();
        setName(name);
        setExpression(expression);
    }
}
