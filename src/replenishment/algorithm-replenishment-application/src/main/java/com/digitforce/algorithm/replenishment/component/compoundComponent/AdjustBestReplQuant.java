package com.digitforce.algorithm.replenishment.component.compoundComponent;

import com.digitforce.algorithm.replenishment.component.Component;

public class AdjustBestReplQuant extends Component {
    private String name = "最佳补货量";
    private String expression =  "max(期望订货量, 最小起订量)";


    @Override
    public void setDefaultVariables() {
        super.setDefaultVariables();
        setVariable("补货单位", 0.0);
        setVariable("最小起订量", 0.0);
    }

    public AdjustBestReplQuant() {
        super();
        setExpression(expression);
        setName(name);
    }



    @Override
    public double postProcess(double value) {
        double unit = getVariable("补货单位");
        double minRepQ = getVariable("最小起订量");
        double netDemand = getVariable("期望订货量");
        double minNetDemand = getVariable("最小订货量");
        double threshold = getVariable("补货阈值");
        double quant = (value - minRepQ) / unit;
        variables.put("补货量", value);
        double quantLowerBound = Math.floor(quant) * unit + minRepQ;
        double quantUpperBound = Math.ceil(quant) * unit + minRepQ;

        // 添加日志
        String equation1 = "(补货量 - 最小起订量) / 补货单位";
        super.processReplLog(equation1, "临时补货量", quant);
        variables.put("临时补货量", quant);
        String equation2 = "Math.floor(临时补货量) * 补货单位 + 最小起订量";
        super.processReplLog(equation2, "下限补货量", quantLowerBound);
        variables.put("下限补货量", quantLowerBound);
        String equation3 =  "Math.ceil(临时补货量) * 补货单位 + 最小起订量";
        super.processReplLog(equation3, "上限补货量", quantUpperBound);
        variables.put("上限补货量", quantUpperBound);

        double adjustValue;
        String equation;
        if (Double.compare(minNetDemand, 0.0) == 0 && netDemand / minRepQ < threshold)  {
            adjustValue = 0.0;
            equation = "因为(最小订货量为0)且(期望订货量 / 最小起订量 < 补货阈值)";
        } else if (quantLowerBound >= minNetDemand && (quant * unit - quantLowerBound)/minRepQ < threshold) {
            adjustValue = quantLowerBound;
            equation = "因为(下限补货量 >= 最小订货量)且((临时补货量 * 补货单位 - 下限补货量) / 最小起订量 < 补货阈值)";
        } else {
            adjustValue = quantUpperBound;
            equation = "因为((最小订货量不为0)或(期望订货量 / 最小起订量 >= 补货阈值))且 ((下限补货量 < 最小订货量)或((临时补货量 * 补货单位 - 下限补货量) / 最小起订量 >= 补货阈值))";
        }
        super.processReplLog(equation, "处理后补货量", adjustValue);
        return adjustValue;
    }
}
