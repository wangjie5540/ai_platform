package com.digitforce.algorithm.replenishment.component.unitComponent;

public class MultiStagedValidStock extends ValidStock{

    String expression =  "当前库存 + 补货在途 + 调拨在途 + 子节点库存和";

    @Override
    protected void setDefaultVariables() {
        super.setDefaultVariables();
        setVariable("子节点库存和", 0.0);
    }

    public MultiStagedValidStock() {
        super();
        setName(name);
        setExpression(expression);
    }
}
