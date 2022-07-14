package com.digitforce.algorithm.dto;

import com.digitforce.algorithm.dto.data.DateProcessor;
import com.digitforce.algorithm.dto.data.SortByLengthComparator;
import com.fasterxml.jackson.annotation.JsonBackReference;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Getter
@Setter
/**
 * 补货日志类
 */
//@AllArgsConstructor
public class ReplenishmentLog {

    private String name;
    /**
     * 子日志
     */
    private Map<String, ReplenishmentLog> subLog;


//    private ReplenishmentLog parentLog;

    /**
     * 计算公式列表
     */
    private List<Formula> formulas;

    private String stage;

    /**
     * 中间结果
     */
    private Map<String, Double> middleRes = new HashMap<>();


    public ReplenishmentLog() {
        subLog = new HashMap<>();
        formulas = new ArrayList<>();
    }

    public void addSubLog(String name, ReplenishmentLog log) {
//        log.setParentLog(this);
        subLog.put(name, log);
    }

    public void addFormula(Formula formula) {
        formulas.add(formula);
    }

    public void addFormula(Map<String, Double> varaibles, String expression, String name, double result) {
        Formula formula = new Formula(varaibles, expression, name, result);
        formulas.add(formula);
    }

//    public double getSafetyStock() {
//        boolean calFlag = true;
//        double res = 0.0;
//        for (Map.Entry<String, ReplenishmentLog> entry:subLog.entrySet()) {
//            res = entry.getValue().getSafetyStock();
//            if (calFlag) {
//                ReplenishmentLog log = entry.getValue();
//                List<Formula> formulas = log.getFormulas();
//                for (Formula formula : formulas) {
//                    Map<String, Double> variables = formula.getParameters();
//                    for (Map.Entry<String, Double> entry1 : variables.entrySet()) {
//                        String name = entry1.getKey();
//                        if ("毛需求_安全库存".equals(name)) {
//                            double safetyStock = entry1.getValue();
//                            calFlag = false;
//                            return safetyStock;
//                        }
//                    }
//                }
//            }
//        }
//        return res;
//    }

    public void parseLog(String skuId, String shopId, boolean writeFlag) {
        try {

            File file = new File("D:\\公司项目\\补货策略\\测试数据\\大仓测试数据\\20220616_20220620\\20220616_20220620补货结果.txt");
            if (!file.exists()) {
                file.createNewFile();
            }

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file, true)));


            if (subLog == null)
                return;

            boolean print = true;
            for (String name : subLog.keySet()) {
                ReplenishmentLog log = subLog.get(name);
                log.parseLog(skuId,shopId, writeFlag);
                // 输出title
                if (log.getSubLog() == null || log.getSubLog().isEmpty() && print) {
                    if (!log.getName().contains("有效库存")) {
                        if (log.getStage()!= null && !log.getStage().isEmpty())
                            System.out.println("-------------------------------------------------------------------------------------");
                        if (log.getStage()!= null && !log.getStage().isEmpty()) {
                            System.out.println("开始计算" + log.getStage() + "下的补货量");
                        }
//                        System.out.println("[" + log.getName().split("_")[0] + "]");
                        if (!log.getName().contains("链路")) {
                            print = false;
                        }
                        if (writeFlag) {
                            bw.newLine();
                            bw.newLine();
                            if (log.getStage()!= null && !log.getStage().isEmpty())
                                bw.write("-------------------------------------------------------------------------------------" + "\r\n");
                            bw.write("开始计算" + skuId + "在仓/店 " + shopId + "补货量" + "\r\n");
                            if (log.getStage()!= null && !log.getStage().isEmpty()) {
                                bw.write("开始计算" + log.getStage() + "下的补货量" + "\r\n");
                            }
                            bw.flush();
                        }
                    }
                    System.out.println("[" + log.getName().split("_")[0] + "]");
                    bw.write("[" + log.getName().split("_")[0] + "]" + "\r\n");
                    bw.flush();
                } else {
                    if (log.getStage() != null) {
                        int subLogStageNum = log.getSubLog().values().stream().map(e -> e.getStage().split("_").length).findAny().get();
                        int stageNum = log.getStage().split("_").length;
                        // 当阶段发生变化时用横线隔开
                        if (stageNum != subLogStageNum) {
                            System.out.println("-------------------------------------------------------------------------------------" + "\r\n");
                            if (writeFlag) {
                                bw.write("-------------------------------------------------------------------------------------" + "\r\n");
                                bw.flush();
                            }
                        }
                    }
                    // 当计算到新模块时打印模块名称

                    if (log.getName() != null) {
                        List<String> subNameList = log.getSubLog().values().stream().map(e -> e.getName().split("_")[0])
                                .collect(Collectors.toList());
                        if (!subNameList.contains(log.getName().split("_")[0])) {
                            System.out.println("[" + log.getName().split("_")[0] + "]");
                            if (writeFlag) {
                                bw.write("[" + log.getName().split("_")[0] + "]" + "\r\n");
                                bw.flush();
                            }
                        }
                    }
                }
                // 公式输出
                for (Formula formula : log.formulas) {
                    Map<String, Double> variables = formula.getParameters();
                    String expression = formula.getExpression();
                    List<String> keys = new ArrayList<>(variables.keySet());
                    Collections.sort(keys, new SortByLengthComparator());

                    List<String> replacedStrings = new ArrayList<>();
                    for (String key : keys) {

                        Double value = variables.get(key);
                        for (String replaced : replacedStrings) {
                            if (key.contains(replaced)) {
                                int first = expression.indexOf(key);
                                String subStr = expression.substring(expression.indexOf(key) + key.length(), expression.length());
                                int start = subStr.indexOf("[");
                                int end = subStr.indexOf("]");
                                int keyLength = key.length();
                                String target = expression.substring(first, first + keyLength) + "\\" + expression.substring(first + keyLength + start, first + keyLength + end) + "\\]";
                                expression = expression.replaceAll(target, key);

                            }
                        }

                        expression = expression.replaceAll(key, key + "[" + String.format("%.2f", value) + "]");
                        replacedStrings.add(key);
                    }
                    System.out.println(formula.getResultName() + " = " + expression + " = " + formula.getValue());
                    if (writeFlag)
                        bw.write(formula.getResultName() + " = " + expression + " = " + formula.getValue() + "\r\n");
                        bw.flush();
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public void postProcessLogPeriod(String date, ReplRequest request) {
        Map<String, ReplenishmentLog> subLog = this.getSubLog();
        for (Map.Entry<String, ReplenishmentLog> entry : subLog.entrySet()) {
            entry.getValue().postProcessLogPeriod(date, request);
            ReplenishmentLog log =entry.getValue();
            String stage = log.getStage();
            int periodADays = (int) Math.ceil(request.getPeriodADays());
            int ldays = (int) Math.ceil(request.getOrderDeliveryDays());
            String periodBStartDate = DateProcessor.addDate(date, ldays);
            String periodAEndDate = DateProcessor.addDate(date, periodADays);
            String updatedStage = stage;
            if (stage != null) {
//                if (!stage.contains("展望期")) {
//                    updatedStage = stage.replaceAll("A", "展望期A");
//                    updatedStage = updatedStage.replaceAll("B", "展望期B");
//                }
                updatedStage = stage.replaceAll("展望期A", "展望期A [" + date + "至" + periodAEndDate + "]");
                updatedStage = updatedStage.replaceAll("展望期B", "展望期B [" + periodBStartDate + "至" + periodAEndDate + "]");

                if (stage.contains("最小服务水平")) {
                    updatedStage = updatedStage.replaceAll("最小服务水平", "最小服务水平 [" + request.getMinServiceLevel() + "]");
                } else {
                    updatedStage = updatedStage.replaceAll("服务水平", "服务水平 [" + request.getServiceLevel() + "]");
                }
            }
            entry.getValue().setStage(updatedStage);
        }
    }


    public void resetLogName(String newNamePart) {
        if (subLog == null)
            return;
        for (String name : subLog.keySet()) {
            ReplenishmentLog log = subLog.get(name);
            log.setName(newNamePart + log.getName());
            log.resetLogName(newNamePart);
        }
    }



    /** 获取需要的中间结果
     *
     * @param middleResMap
     * @return
     */
    public Map<String, Double> processMidResult(Map<String, String> middleResMap, Map<String, Double> middleRes) {

        for (Map.Entry<String, ReplenishmentLog> entry : subLog.entrySet()) {
            ReplenishmentLog log = entry.getValue();
            List<Formula> formulas = log.getFormulas();
            String stage = log.getStage();
            for (Formula formula:formulas) {
                Map<String, Double> vairables = formula.getParameters();
                for (Map.Entry<String, String> e:middleResMap.entrySet()) {
                    if (vairables.keySet().contains(e.getValue())) {
                        double value = vairables.getOrDefault(e.getValue(), 0.0);
                        middleRes.put(stage + e.getKey(), value);
                    }
                }
            }
            log.processMidResult(middleResMap, middleRes);
        }
        return middleRes;
    }




}
