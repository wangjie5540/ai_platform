package com.digitforce.algorithm.replenishment.component.unitComponent;

import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.replenishment.component.Component;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.TDistribution;

public class ResidualSafetyStock extends Component {
    String name = "毛需求_安全库存";

    String expression = "min(T值 * sqrt(方差估计), 毛需求_安全库存下界)";

    @Override
    public void setDefaultVariables() {
        setVariable("服务水平", 0.95);
        setVariable("样本数量", 0);
        setVariable("方差估计", 0);
    }

    public ResidualSafetyStock() {
        super();
        setName(name);
        setExpression(expression);
    }

    @Override
    public void preProcess() {
        AbstractRealDistribution dist;
        double dof = getVariable("样本数量");
        double sl = getVariable("服务水平");
        double p = 0.5 + sl/2.0;
        if(dof > 0) {
            dist = new TDistribution(dof);
        }
        else {
            dist = new NormalDistribution();

        }
        double Tvalue = dist.inverseCumulativeProbability(p);
        String equation = "获取T值";
        super.processReplLog(equation, "T值", Tvalue);
        setVariable("T值", Tvalue);
    }



}
