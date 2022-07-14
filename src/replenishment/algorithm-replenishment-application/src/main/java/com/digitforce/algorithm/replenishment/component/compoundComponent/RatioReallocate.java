package com.digitforce.algorithm.replenishment.component.compoundComponent;

import com.digitforce.algorithm.replenishment.component.Component;

public class RatioReallocate extends Component {
    String name = "分配补货量";
    String expression = "上级库存 * 补货量比例";

    @Override
    protected void setDefaultVariables() {
        super.setDefaultVariables();
        setVariable("上级库存", 0.0);
        setVariable("补货量比例", 0.0);
    }


}
