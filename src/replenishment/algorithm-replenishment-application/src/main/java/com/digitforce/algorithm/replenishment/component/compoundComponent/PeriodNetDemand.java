package com.digitforce.algorithm.replenishment.component.compoundComponent;

import com.digitforce.algorithm.replenishment.component.Component;

public class PeriodNetDemand extends Component {
    String name = "期望补货量";

    String expression = "min(max(展望期A净需求, 0), 展望期B净需求)";


    public PeriodNetDemand() {
        super();
        setName(name);
        setExpression(expression);
    }
}
