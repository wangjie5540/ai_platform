package com.digitforce.algorithm.replenishment.component.grossDemand;

import com.digitforce.algorithm.replenishment.component.Component;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderPointGrossDemand extends Component {

    String name = "毛需求";

    String expression = "max(毛需求_起订点, 最小陈列量)";

    @Override
    public void setDefaultVariables() {
        setVariable("最小陈列量", 0);
        setVariable("最低库存", 0);
        setVariable("判断是否补货", 0);
        setVariable("库内库存", 0);
        setVariable("在途库存", 0);
    }

    public OrderPointGrossDemand() {
        super();
        setName(name);
        setExpression(expression);
    }


    @Override
    public double postProcess(double value) {
        double ceiledValue = Math.ceil(value);
        String equation = "Math.ceil(毛需求)";
        processReplLog(equation, "毛需求", ceiledValue);
        return Math.ceil(value);
    }


    @Override
    public boolean quitCal() {
        double replOrNot = getVariable("判断是否补货");
        // 需要判断补货
        if (Double.compare(replOrNot, 1) == 0) {
            double orderPoint = getVariable("毛需求_起订点");
            double stock = getVariable("库内库存");
            double transferStock = getVariable("在途库存");
            double replQ = orderPoint - stock -transferStock;
            boolean flag;
            if (replQ > 0) {
                flag = false;
            } else {
                log.info("库存充足,无需补货");
                String equation = "因为库内库存 + 在途库存 >= 毛需求_起订点";
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
