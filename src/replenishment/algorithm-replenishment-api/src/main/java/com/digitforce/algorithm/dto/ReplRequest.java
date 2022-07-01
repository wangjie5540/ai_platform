package com.digitforce.algorithm.dto;

import lombok.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ReplRequest implements Cloneable{

    /**
     * 商品编码
     */
    private String skuId;

    /**
     * 商品名称
     */
    private String skuName;

    /**
     * 商品属性：常规品，低消品等
     */
    private String skuProperty;

    /**
     * 均销量
     */
    private Double dms;

    /**
     * 展望期与服务水平对应的残差
     */
    // key格式为 A，0.9,A代表展望期A，0.9代表服务水平
    private Map<String, Double> residuals;

    /**
     * 不同时期的预测销量
     */
    private Map<String, Double> predictedSales;

    /**
     * 展望期A天数
     */
    private Double periodADays;

    /**
     * 展望期B天数
     */
    private Double periodBDays;

    /**
     * 展望期A预测销量
     */
    private Double periodASales;

    /**
     * 展望期B预测销量
     */
    private Double periodBSales;


    /**
     * 服务水平
     */
    private Double serviceLevel;

    /**
     * 最小服务水平
     */
    private Double minServiceLevel;

    /**
     * 最小库存
     */
    private Double minInStock;

    /**
     * 最小排面量
     */
    private Double minDisplayRequire;

    /**
     * 库存量
     */
    private Double stock;

    /**
     * 在途库存
     */
    private Double transferStock;

    /**
     * 安全库存量或安全库存天数
     */
    private Double safetyStockDays;

    /**
     * 安全库存量
     */
    private Double safetyStock;
    /**
     * 最小订货量
     */
    private Double minOrderQ;

    /**
     * 补货单位
     */
    private Double unit;

    /**
     * 展望期方差
     */
    private Double periodDaysVariance;

    /**
     * 质保期
     */
    private Double warrantyPeriods;

    /**
     * 库存成本
     */
    private Double inventoryCost;

    /**
     * 补货成本
     */
    private Double replenishmentCost;


    /**
     * 过去30天销量
     */
    private List<Double> history30DaysSales;


    /**
     * 商品部门id
     */
    private String deptId;

    /**
     * 历史7天均销量
     */
    private Double last7DaysAvgSales;

    /**
     * 历史30天均销量
     */
    private Double last30DaysAvgSales;

    /**
     * 未来7天预测销量均值
     */
    private Double in7DaysAvgSales;

    /**
     * 样本数量
     */
    private Double sampleNum;

    /**
     * 展望期A方差评估
     */
    private Double periodAEstimateVariance;

    /**
     * 展望期B方差评估
     */
    private Double periodBEstimateVariance;

    /**
     * 销售期
     */
    private Double salesPeriod;

    /**
     * 商店id
     */
    private String shopId;

    /**
     * 安全库存模型
     */
    private String safetyStockModel;

    /**
     * 补货模型
     */
    private String replenishModel;


    /**
     * 可订货日
     */
    private String orderableDates;

    /**
     * 补货提前期
     */
    private Integer replAdvancedDate;

    /**
     * 历史30天销量长度
     */
    private Integer history30DaysSalesLength;

    /**
     * 是否为爆品
     */
    private Boolean bestSellLabel;

    /**
     *下单到收货日的天数
     */
    private Double orderDeliveryDays;

    /**
     * 是否为新品
     */
    private Boolean isNew;

    /**
     * 是否为短保
     */
    private Boolean isShortWarranty;

    /**
     * 是否为标品
     */
    private Boolean isStandard;

    /**
     * boostrap分位数
     */
    private double percentile;

    /**
     * 每个链路子节点将有库存和(库内在在途）
     */
    private Map<String, Double> branchNodeStock;

    /**
     * 展望期A每个链路下门店预测销量
     */
    private Map<String, Double> branchPeriodASales;

    /**
     * 展望期B每个链路下门店预测销量
     */
    private Map<String, Double> branchPeriodBSales;

    /**
     * 展望期A每个链路下销售残差
     */
    private Map<String, Double> branchPeriodAVariance;

    /**
     * 展望期B每个链路下销售残差
     */
    private Map<String, Double> branchPeriodBVariance;

    /**
     * 子节点所有库存+在途
     */
    private Double nodeStock;

    /**
     * 补货在途
     */
    private Double replQInTransit;

    /**
     * key对应链路j, ReplRequest是j门店在第n层的对象，其成员变量销量为j门店在未来n层对应的展望期的预测销量；
     * 库存为j门店在n层对应的库内库存stock为j门店<=n层所有门店的库内库存
     * 在途库存为j门店在n层对应的在途库存transferStock为j门店<=n层所有门店的在途库存
     */
    private Map<String, ReplRequest> branchRequests;

    private Integer level;

    private String date;

    /**
     * 进价
     */
    private Double purPrice;

    public void setDefaultValue() {
        this.skuId = "";
        this.skuName = "";
        this.skuProperty = "normal";
        this.dms = 0.0;
        this.periodADays = 0.0;
        this.periodBDays = 0.0;
        this.serviceLevel = 0.95;
        this.minServiceLevel = 0.85;
        this.periodASales = 0.0;
        this.periodBSales = 0.0;
        this.minInStock = 0.0;
        this.minDisplayRequire = 0.0;
        this.stock = 0.0;
        this.transferStock = 0.0;
        this.replQInTransit = 0.0;
        this.safetyStockDays = 2.0;
        this.safetyStock = 0.0;
        this.minOrderQ = 0.0;
        this.unit = 1.0;
        this.periodDaysVariance = 0.0;
        this.warrantyPeriods = 0.0;
        this.inventoryCost = 0.0;
        this.replenishmentCost = 0.0;
        this.history30DaysSales = new ArrayList<>();
        this.deptId = "";
        this.last7DaysAvgSales = 0.0;
        this.last30DaysAvgSales = 0.0;
        this.in7DaysAvgSales = 0.0;
        this.sampleNum = 0.0;
        this.periodAEstimateVariance = 0.0;
        this.periodBEstimateVariance = 0.0;
        this.salesPeriod = 0.0;
        this.shopId = "";
        this.safetyStockModel = "";
        this.replenishModel = "";
        this.orderableDates = "";
        this.replAdvancedDate = 0;
        this.history30DaysSalesLength = 0;
        this.bestSellLabel = false;
        this.orderDeliveryDays = 0.0;
        this.isNew = false;
        this.isShortWarranty = false;
        this.isStandard = false;
        this.nodeStock = 0.0;
        this.branchNodeStock = new HashMap<>();
        this.branchPeriodASales = new HashMap<>();
        this.branchPeriodBSales = new HashMap<>();
        this.branchPeriodAVariance = new HashMap<>();
        this.branchPeriodBVariance = new HashMap<>();
        this.branchRequests = new HashMap<>();
        this.level = 0;
        this.date = "";
        this.purPrice = 0.0;
    }

    public ReplRequest clone() {
        ReplRequest request = null;
        try {
            request = (ReplRequest) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return request;
    }
}
