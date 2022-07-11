package com.digitforce.algorithm.replenishment.component.unitComponent;

import com.digitforce.algorithm.replenishment.component.Component;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrossDemand extends Component {
    String name = "毛需求";
    String expression = "max(min(销量期望+毛需求_安全库存+附加需求, 需求下限), 需求上限)";

    @Override
    protected void setDefaultVariables() {
        super.setDefaultVariables();
        setVariable("销量期望", 0.0);
        setVariable("安全库存", 0.0);
        setVariable("附加需求", 0.0);
        setVariable("需求下限", Double.MAX_VALUE);
        setVariable("需求上限", 0.0);
    }

    public GrossDemand() {
        super();
        setName(name);
        setExpression(expression);
    }

    public GrossDemand(String expression) {
        super();
        setExpression(expression);
        setName(name);
    }

//    @Override
//    public boolean quitCal() {
//        double replOrNot = getVariable("判断是否补货");
//        // 需要判断补货
//        if (Double.compare(replOrNot, 1) == 0) {
//            double orderPoint = getVariable("毛需求");
//            double stock = getVariable("库内库存");
//            double transferStock = getVariable("在途库存");
//            double replQ = orderPoint - stock -transferStock;
//            boolean flag;
//            if (replQ > 0) {
//                flag = false;
//            } else {
//                log.info("库存充足,无需补货");
//                String equation = "因为库内库存 + 在途库存 >= 毛需求";
//                processReplLog(equation, "补货量", 0.0);
//                flag = true;
//            }
//            if (this.parent != null)
//                this.parent.setQuitFlag(flag);
//            return flag;
//        }
//        return false;
//    }

    @Override
    public double postProcess(double value) {
        setVariable("毛需求", value);
        return value;
    }
}
