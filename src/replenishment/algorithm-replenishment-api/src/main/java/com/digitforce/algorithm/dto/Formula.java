package com.digitforce.algorithm.dto;


import lombok.*;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
//@AllArgsConstructor
public class Formula {
    /**
     * 计算公式
     */
    private String expression;
    /**
     * 参数值
     */
    private Map<String, Double> parameters;
    /**
     * 结果名
     */
    private String resultName;
    /**
     * 计算结果值
     */
    private Double value;

    public Formula(Map<String, Double> variables, String expression, String name, Double value) {
        parameters = new HashMap<>();
        if (variables != null) {
            for (Map.Entry<String, Double> entry : variables.entrySet()) {
                if (expression.contains(entry.getKey())) {
                    parameters.put(entry.getKey(), entry.getValue());
                }
            }
        }
        this.expression = expression;
        this.resultName = name;
        this.value = value;
    }




    public void addParameters(String name, Double value) {
        parameters.put(name, value);
    }



}
