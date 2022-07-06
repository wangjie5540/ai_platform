package com.digitforce.algorithm.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class StockByShopDate {
    private String goodsId;

    private String shopId;

    /**
     * 每日期初库存
     */
    private double dateIntialStock;

    private double dateAmtEnd;

    private double dateAmtStart;

    /**
     * 每日期末库存
     */
    private double dateEndStock;


    private String date;
}
