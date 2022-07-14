package com.digitforce.algorithm.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class TransferStock {
    private String goodsId;

    private String shopId;

    private double transferStock;

    /**
     * 预计到达日期
     */
    private String arrivalDate;

    /**
     * 下单日期
     */
    private String orderDate;
}
