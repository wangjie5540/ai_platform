package com.digitforce.algorithm.replenishment.component.demand;

public class NewGoodsOrderPoint extends OrderPoint {

    @Override
    public void setDefaultVariables() {
        super.setDefaultVariables();
        setVariable("展望期天数", 0.0);
    }

    @Override
    public void preProcess() {
        super.preProcess();
        double dms = getVariable("日销量");
        double periodDays = getVariable("展望期天数");
        double sales = dms * periodDays;
        setVariable("展望期销量预测", sales);
        String equation = "日销量 * 展望期天数";
        processReplLog(equation, "展望期销量预测", sales);
    }



}
