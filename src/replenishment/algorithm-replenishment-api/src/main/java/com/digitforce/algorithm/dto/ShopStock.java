package com.digitforce.algorithm.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class ShopStock implements Cloneable{

    /**
     * 商品id
     */
    private String goodsId;

    /**
     * 日期
     */
    private String date;

    /**
     * 店/仓id
     */
    private String shopId;

    /**
     * 门店库存
     */
    private double shopStock;

    public Object clone() {
        Object ob = null;
        try {
            ob = (ShopStock) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return ob;
    }
}
