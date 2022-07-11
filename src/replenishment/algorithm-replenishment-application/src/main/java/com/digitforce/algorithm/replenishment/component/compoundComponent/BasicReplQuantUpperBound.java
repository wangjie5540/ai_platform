package com.digitforce.algorithm.replenishment.component.compoundComponent;

import com.digitforce.algorithm.dto.ReplenishmentLog;
import com.digitforce.algorithm.replenishment.component.Component;

import java.util.List;

/**
 * 计算多级补货每个分支下的补货量之和
 */
public class BasicReplQuantUpperBound extends BasicReplQuant {
    String name = "需求上限";

    @Override
    public void preProcess() {
        double branchBaseReplQuantSum = 0.0;
        List<Component> componentList = (List<Component>) getParameters().get("链路补货量列表");
        Component validStock =(Component) getParameters().get("有效库存");
        int root = 1;
        String equation = "";
        for (Component component:componentList) {
            double value = component.calc();
            branchBaseReplQuantSum += value;
            String newNamePrefix = "链路" + root;
            ReplenishmentLog subLog = component.getReplLog();
            subLog.setName(newNamePrefix + subLog.getName());
            subLog.resetLogName(newNamePrefix);
            this.getReplLog().addSubLog(newNamePrefix + component.getName(), subLog);
            equation += newNamePrefix + "期望订货量 + " ;
            root += 1;
        }

        super.processReplLog(equation.substring(0, equation.length()-1), "毛需求", branchBaseReplQuantSum);
        setVariable("毛需求", branchBaseReplQuantSum);
        double stock = validStock.calc();
        setVariable("有效库存", stock);
    }


    public BasicReplQuantUpperBound() {
        super();
        setName(name);
    }
}
