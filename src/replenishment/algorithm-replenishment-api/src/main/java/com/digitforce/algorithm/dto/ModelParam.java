package com.digitforce.algorithm.dto;


import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class ModelParam {

    /**
     * 新品安全库存系数, key为条件量，value为条件值与参数值，例如key为可售期，value为Map<0-60, 0.5>
     * 则为可售期再0-60天内的安全库存系数为0.5
     */
    private Map<String, Map<String, Double>> newProdSafetyStockCoef;

    private Double residualSafetySotckBoundCoef;

    private Double residualSafetyStockT;



}
