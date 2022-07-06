package com.digitforce.algorithm.replenishment.component.unitComponent;

import com.digitforce.algorithm.replenishment.component.Component;

public class SafetyStockBound extends Component {
    String name = "毛需求_安全库存下界";
    String expression = "min(min(过去7天日均销, 过去30天日均销), 未来7天预测日均销) * (安全库存限制系数 + 展望期天数)";

    @Override
    public void setDefaultVariables() {
        setVariable("过去7天日均销", 0);
        setVariable("过去30天日均销", 0);
        setVariable("未来7天预测日均销", 0);
        setVariable("安全库存限制系数", 0);
        setVariable("展望期天数", 0);
    }

    public SafetyStockBound() {
        super();
        setName(name);
        setExpression(expression);
    }
}
