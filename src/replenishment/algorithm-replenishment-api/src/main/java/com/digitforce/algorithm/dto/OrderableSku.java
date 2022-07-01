package com.digitforce.algorithm.dto;

import lombok.Getter;

@Getter
public class OrderableSku {

    /**
     * skuId
     */
    private String skuId;

    /**
     * 日期
     */
    private String date;

    /**
     * 门店id
     */
    private String shopId;

    /**
     * 可订货日
     */
    private String orderableDates;
}
