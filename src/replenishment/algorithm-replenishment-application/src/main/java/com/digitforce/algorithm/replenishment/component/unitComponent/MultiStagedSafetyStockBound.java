package com.digitforce.algorithm.replenishment.component.unitComponent;

import com.digitforce.algorithm.dto.ReplenishmentLog;
import com.digitforce.algorithm.replenishment.component.Component;

import java.util.List;

public class MultiStagedSafetyStockBound extends Component {
    String name = "毛需求_安全库存下界";
    String expression = name;

    public MultiStagedSafetyStockBound() {
        super();
        setName(name);
        setExpression(expression);
    }

    @Override
    public void preProcess() {
        List<Component> branchSafetyStockBounds = (List<Component>) getParameters().get("链路安全库存下界");
        double boundValue = 0.0;
        String equation = "";
        int root = 1;
        for (Component component:branchSafetyStockBounds) {
            double value=  component.calc();
            boundValue += value;
            String prefix = "链路" + root;
//            ReplenishmentLog subLog = component.getReplLog();
//            subLog.setName(prefix + subLog.getName());
//            subLog.resetLogName(prefix);
//            this.getReplLog().addSubLog(prefix + component.getName(), subLog);
            equation += prefix + name + "+";
            root += 1;
        }
        setVariable(name, boundValue);
//        super.processReplLog(equation.substring(0, equation.length()-1), name, boundValue);
    }
}
