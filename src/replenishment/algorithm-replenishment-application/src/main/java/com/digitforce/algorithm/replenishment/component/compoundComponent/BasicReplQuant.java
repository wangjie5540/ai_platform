package com.digitforce.algorithm.replenishment.component.compoundComponent;

import com.digitforce.algorithm.replenishment.component.Component;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicReplQuant extends Component {
    String name = "期望订货量";

    String expression = "max(毛需求 - 有效库存, EOQ)";
    @Override
    protected void setDefaultVariables() {
        super.setDefaultVariables();
        setVariable("EOQ", 0.0);
        setVariable("判断是否补货", 0.0);
    }

    public BasicReplQuant() {
        super();
        setName(name);
        setExpression(expression);
    }

    @Override
    public boolean quitCal() {
        double replOrNot = getVariable("判断是否补货");
        // 需要判断补货
        if (Double.compare(replOrNot, 1) == 0) {
            double orderPoint = getVariable("毛需求");
            double validStock = getVariable("有效库存");
            double replQ = orderPoint - validStock;
            boolean flag;
            if (replQ > 0) {
                flag = false;
            } else {
//                log.info("库存充足,无需补货");
                String equation = "有效库存 >= 毛需求";
                processReplLog(equation, "补货量", 0.0);
                flag = true;
            }
            if (this.parent != null)
                this.parent.setQuitFlag(flag);
            return flag;
        }
        return false;
    }
}
