package com.digitforce.algorithm.replenishment.component.demand;


import com.digitforce.algorithm.replenishment.component.Component;

public class OrderPoint extends Component {
    String name = "毛需求_起订点";
    String expression = "max(最低库存, 展望期销量预测 + 毛需求_安全库存)";

    @Override
    public void setDefaultVariables() {
        setVariable("最低库存", 0);
        setVariable("展望期销量预测", 0);
        setVariable("日销量", 0);
        setVariable("安全库存天数", 0);
    }

    public OrderPoint() {
        super();
        setName(name);
        setExpression(expression);

    }

    @Override
    public void preProcess() {
        double dms = getVariable("日销量");
        double safetyStockDays = getVariable("安全库存天数");
        double safetyStock = dms * safetyStockDays;
        setVariable("毛需求_安全库存", safetyStock);
        String equation = "日销量 * 安全库存天数";
        processReplLog(equation, "毛需求_安全库存", safetyStock);
    }


    @Override
    public double postProcess(double value) {
        double postProcessValue = Math.ceil(value);
        setVariable(name, value);
        String equation = "Math.ceil(毛需求_起订点)";
        processReplLog(equation, name, postProcessValue);
        return postProcessValue;
    }


}
