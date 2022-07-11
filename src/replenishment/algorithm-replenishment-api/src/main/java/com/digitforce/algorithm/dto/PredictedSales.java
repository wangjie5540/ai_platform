package com.digitforce.algorithm.dto;

import lombok.Getter;

@Getter
public class PredictedSales extends Sales{

    private String hourType;

    private int predDays;

    private String model;

    public PredictedSales(String goodsId, double salesQ, String date, String shopId, String hourType, int predDays) {
        super(goodsId, salesQ, date, shopId);
        this.hourType = hourType;
        this.predDays = predDays;
    }
}
