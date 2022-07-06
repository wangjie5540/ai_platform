package com.digitforce.algorithm.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
public class ReplResult{

    /**
     * 最终补货建议量
     */
    private Double replQuant;

    /**
     * 补货日志
     */
    private ReplenishmentLog replenishmentLog;

    /**
     * 补货策略
     */
    private String strategy;

    /**
     * 安全库存
     */
    private Map<String, Double> middleRes;

    private String shopId;

    private String goodsId;

    private String date;

    public void printLog(boolean writeFlag) {
        System.out.println("选择策略:" + strategy);
        replenishmentLog.parseLog(goodsId, shopId, writeFlag);
    }

    public void setMiddleRes(Map<String, Double> middleRes) {
        this.middleRes = middleRes;
    }

    public ReplResult(Double replQuant, String strategy, String shopId, String goodsId) {
        this.replQuant = replQuant;
        this.strategy = strategy;
        this.shopId = shopId;
        this.goodsId = goodsId;
    }
}
