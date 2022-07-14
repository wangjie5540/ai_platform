package com.digitforce.algorithm.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class ReplenishmentServiceParam {

    /**
     * 商品列表
     */
    private List<ReplRequest> replRequestList;

    /**
     * 模型参数
     */
    private ModelParam modelParam;

    /**
     * 父节点
     */
    private ParentNode parentNode;

    /**
     * 日期
     */
    private String date;

    /**
     * 输出日志标志
     */
    private Boolean logFlag;

    /**
     * 计算多级补货标志
     */
    private Integer replProcess;
}
