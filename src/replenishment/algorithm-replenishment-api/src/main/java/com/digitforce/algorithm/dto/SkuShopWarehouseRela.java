package com.digitforce.algorithm.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SkuShopWarehouseRela {
    private String goodsId;

    private String shopId;

    private String supId;

    private String date;

    private String orderableDates;
}
