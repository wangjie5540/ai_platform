package com.digitforce.algorithm.replenishment.component.unitComponent;


import com.digitforce.algorithm.replenishment.component.Component;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class BoostrapSafetyStock extends Component{
    String name = "毛需求_安全库存";
    String expression = "从历史销量数据中滚动抽样获取起订点";

    @Override
    public void setDefaultVariables() {
        setVariable("展望期天数", 0.0);
    }

    @Override
    protected void setDefaultParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("过去30天销量", new ArrayList<>());
        setParameters(parameters);
    }

    public BoostrapSafetyStock() {
        setName(name);
        setExpression(expression);
    }

    @Override
    public void preProcess() {
        List<Double> periodSales = new ArrayList<>();
        double boostrapValue;
        List<Double> yList = (List<Double>) getParameters().get("过去30天销量");
        log.info("过去30天销量:{}", yList);
        double periodDays = getVariable("展望期天数");
        log.info("展望期天数:{}", periodDays);
        if (yList == null || yList.isEmpty()) {
            boostrapValue = 0.0;
        } else if (yList.size() < periodDays) {
            double historySalesSum = yList.stream().mapToDouble(e->e).sum();
            log.info("过去30天销量和:{}; 展望期天数:{}", historySalesSum, periodDays);
            boostrapValue = historySalesSum / yList.size() * periodDays;
        } else {
            for (int i = 0; i < yList.size(); i++) {
                int j = i + (int) periodDays;
                if (j <= yList.size()) {
                    double sales = yList.subList(i, j).stream().mapToDouble(e -> Double.valueOf(e)).sum();
                    periodSales.add(sales);
                }
            }
            log.info("periodSales:{}", periodSales);
            int r = new Random(100).nextInt(periodSales.size());
            boostrapValue = periodSales.get(r);
        }
        setVariable("从历史销量数据中滚动抽样获取起订点", boostrapValue);
    }


}
