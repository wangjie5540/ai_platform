package com.digitforce.algorithm.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class Sales {

    private String goodsId;

    private double salesQ;

    private String date;

    private String shopId;
}
