package com.digitforce.algorithm.replenishment.component.unitComponent;

import com.digitforce.algorithm.replenishment.component.Component;

public class ValidStock extends Component {
    String name = "有效库存";
    String expression = "当前库存 + 补货在途 + 调拨在途";

    @Override
    protected void setDefaultVariables() {
        setVariable("当前库存", 0.0);
        setVariable("补货在途", 0.0);
        setVariable("调拨在途", 0.0);
    }

    public ValidStock() {
        super();
        setName(name);
        setExpression(expression);
    }
}
