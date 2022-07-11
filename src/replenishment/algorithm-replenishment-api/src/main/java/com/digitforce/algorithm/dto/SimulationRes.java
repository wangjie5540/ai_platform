package com.digitforce.algorithm.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SimulationRes extends StockByShopDate{

    public SimulationRes(String goodsId, String shopId, double dateIntialStock, double dateAmtEnd, double dateAmtStart, double dateEndStock, String date) {
        super(goodsId, shopId, dateIntialStock, dateAmtEnd, dateAmtStart, dateEndStock, date);
    }

    /**
     * 需求量
     */
    private double demandQuant;

    /**
     * 缺货量
     */
    private double stockoutQuant;

    /**
     * 缺货标志
     */
    private boolean stockOutFlag;


}
