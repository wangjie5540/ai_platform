package com.digitforce.algorithm.replenishment.component.unitComponent;

import com.digitforce.algorithm.dto.Formula;
import com.digitforce.algorithm.replenishment.component.Component;
import com.digitforce.algorithm.replenishment.component.demand.NewGoodsOrderPoint;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class NewGoodsafetyStock extends Component {
    String name = "毛需求_安全库存";
    String expression = "毛需求_安全库存";

    @Override
    public void setDefaultVariables() {
        super.setDefaultVariables();
        setVariable("毛需求_安全库存", 0.0);
        setVariable("保质期", 0.0);
        setVariable("售卖期", 0.0);
    }

    public NewGoodsafetyStock() {
        super();
        setName(name);
        setExpression(expression);
    }

    @Override
    public void preProcess() {
//        log.info("使用质保期计算新品安全库存");
        Map<String, Map<String, Double>>  newProdCoef = (Map<String, Map<String,Double>> ) getParameters().get("新品安全库存参数");
        double warrantyPeriod = getVariable("保质期");
        double salesPeriod = getVariable("可售期");
        double periodSales = getVariable("销量期望");
        double periodADays = getVariable("展望期天数");
//        log.info("售卖期:{}, 保质期:{}", salesPeriod, warrantyPeriod);
        boolean flag = false;
        double safetyStock = 0.0;
        String equationName = "毛需求_安全库存";
        try {
            boolean loopFlag =  true;
            double coefficient = 0.3;
            for (Map.Entry<String, Map<String, Double>> entry:newProdCoef.entrySet()) {

                double comparasion;

                if ("保质期".equals(entry.getKey())) {
                    comparasion = warrantyPeriod;
                } else {
                    comparasion = salesPeriod;
                }
                for (Map.Entry<String, Double> subEntry : entry.getValue().entrySet()) {
                    if (loopFlag) {
                        String condition = subEntry.getKey();
                        if (condition.contains(">")) {
                            double upper = Double.parseDouble(condition.split(">")[1]);
                            if (Double.compare(comparasion, upper) > 0) {
                                flag = true;
                            }
                        } else if (condition.contains("<")) {
                            double lower = Double.parseDouble(condition.split("<")[1]);
                            if (Double.compare(lower, comparasion) > 0) {
                                flag = true;
                            }
                        } else if (condition.contains("-")) {
                            double lower = Double.parseDouble(condition.split("-")[0]);
                            double upper = Double.parseDouble(condition.split("-")[1]);
                            if (Double.compare(comparasion, lower) >= 0 &&
                                    Double.compare(upper, comparasion) > 0) {
                                flag = true;
                            }
                        } else {
                            double value = Double.parseDouble(condition);
                            if (Double.compare(comparasion, value) == 0) {
                                flag = true;
                            }
                        }

                        if (flag) {
                            coefficient = subEntry.getValue();
                            loopFlag = false;
                        }
                    }
                }

            }
            String equation;
            if (Double.compare(salesPeriod, 1) == 0) {
                safetyStock = periodSales * coefficient;
                equation = "销量期望 * 系数";
            } else {
                safetyStock = periodSales / periodADays * coefficient;
                equation = "销量期望 / 展望期天数 * 系数";
            }
//            log.info("安全库存系数:{}", newProdCoef);
//            log.info("质保期:{}, 销售期:{}, 系数:{}", warrantyPeriod, salesPeriod, coefficient);
            Formula formula = new Formula(variables, equation, equationName, safetyStock);
            formula.addParameters("系数", coefficient);
            this.getReplLog().addFormula(formula);
            setVariable(equationName, safetyStock);

        } catch (Exception e) {
            log.error("新品安全库存:{}系数解析失败:{}", newProdCoef, e);
            setVariable(equationName, 0);
        }

    }



}
