package com.digitforce.algorithm.dto.data;

import com.digitforce.algorithm.dto.ReplRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class PreprocessData {

    public static double calMinServiceLevel(ReplRequest request) {
        double salesPeriod = request.getSalesPeriod();
        boolean bestSellLabel = request.getBestSellLabel();
        double minServiceLevel = 0.75;
        if (Double.compare(salesPeriod, 2) >= 0) {
            minServiceLevel = bestSellLabel ? 0.75:0.6;
        }
//        log.info("skuId:{}, 销售期:{}, 是否为爆品:{}, 最小服务水平:{}", request.getSkuId(),request.getSalesPeriod(),request.getBestSellLabel(), minServiceLevel);
        return minServiceLevel;
    }


    public static double calServiceLevel(ReplRequest request){
        double salesPeriod = request.getSalesPeriod();
        boolean bestSellLabel = request.getBestSellLabel();
        double serviceLevel = 0.975;
        if (Double.compare(salesPeriod, 5) >= 0) {
            serviceLevel = bestSellLabel?0.975:0.95;
        } else if (Double.compare(salesPeriod, 3) >= 0) {
            serviceLevel = bestSellLabel ? 0.95:0.9;
        } else if (Double.compare(salesPeriod, 2) >= 0) {
            serviceLevel = bestSellLabel ? 0.9:0.85;
        } else if (Double.compare(salesPeriod, 1) == 0) {
            serviceLevel = bestSellLabel ? 0.85:0.75;
        }
//        log.info("skuId:{}, 销售期:{}, 是否为爆品:{}, 正常服务水平:{}", request.getSkuId(),request.getSalesPeriod(), request.getBestSellLabel(),
//                serviceLevel);
        return serviceLevel;
    }



    public static ReplRequest processReplRequest(ReplRequest param) {
        ReplRequest request =new ReplRequest();
        request.setDefaultValue();
        if (param.getSkuId() != null && !param.getSkuId().isEmpty()) {
            request.setSkuId(param.getSkuId());
        }

        if (param.getSkuName() != null && !param.getSkuName().isEmpty()) {
            request.setSkuName(param.getSkuName());
        }

        if (param.getSkuProperty() != null && !param.getSkuProperty().isEmpty()) {
            request.setSkuProperty(param.getSkuProperty());
        }

        if (param.getDms() != null && param.getDms() != 0.0) {
            request.setDms(param.getDms());
        }

        if (param.getPeriodADays() != null && param.getPeriodADays() != 0.0) {
            request.setPeriodADays(param.getPeriodADays());
        }
        if (param.getPeriodBDays() != null && param.getPeriodBDays() != 0.0) {
            request.setPeriodBDays(param.getPeriodBDays());
        }

        if (param.getOrderDeliveryDays() != null && param.getOrderDeliveryDays() != 0.0) {
            request.setOrderDeliveryDays(param.getOrderDeliveryDays());
        }
        if (param.getPeriodASales() != null && param.getPeriodASales() != 0.0) {
            request.setPeriodASales(param.getPeriodASales());
        }
        if (param.getPeriodBSales() != null && param.getPeriodBSales() != 0.0) {
            request.setPeriodBSales(param.getPeriodBSales());
        }
        if (param.getMinInStock() != null && param.getMinInStock() != 0.0) {
            request.setMinInStock(param.getMinInStock());
        }
        if (param.getMinDisplayRequire() != null && param.getMinDisplayRequire() != 0.0) {
            request.setMinDisplayRequire(param.getMinDisplayRequire());
        }
        if (param.getStock() != null && param.getStock() != 0.0) {
            request.setStock(param.getStock());
        }
        if (param.getTransferStock() != null && param.getTransferStock() != 0.0) {
            request.setTransferStock(param.getTransferStock());
        }

        if (param.getReplQInTransit() != null && param.getReplQInTransit() != 0.0) {
            request.setReplQInTransit(param.getReplQInTransit());
        }

        if (param.getSafetyStock() != null && param.getSafetyStockDays() != 0.0) {
            request.setSafetyStockDays(param.getSafetyStockDays());
        }

        if (param.getMinOrderQ() != null && param.getMinOrderQ() != 0.0) {
            request.setMinOrderQ(param.getMinOrderQ());
        }

        if (param.getUnit() != null && param.getUnit() != 0.0) {
            request.setUnit(param.getUnit());
        }

        if (param.getPeriodDaysVariance() != null && param.getPeriodDaysVariance() != 0.0) {
            request.setPeriodDaysVariance(param.getPeriodDaysVariance());
        }

        if (param.getWarrantyPeriods() != null && param.getWarrantyPeriods() != 0.0) {
            request.setWarrantyPeriods(param.getWarrantyPeriods());
        }

        if (param.getInventoryCost() != null && param.getInventoryCost() != 0.0) {
            request.setInventoryCost(param.getInventoryCost());
        }

        if (param.getReplenishmentCost() != null && param.getReplenishmentCost() != 0.0) {
            request.setReplenishmentCost(param.getReplenishmentCost());
        }

        if (param.getHistory30DaysSales() != null && !param.getHistory30DaysSales().isEmpty()) {
            request.setHistory30DaysSales(param.getHistory30DaysSales());
        }

        if (param.getDeptId() != null && !param.getDeptId().isEmpty()) {
            request.setDeptId(param.getDeptId());
        }


        if (param.getLast7DaysAvgSales() != null && param.getLast7DaysAvgSales() != 0.0) {
            request.setLast7DaysAvgSales(param.getLast7DaysAvgSales());
        }

        if (param.getLast30DaysAvgSales() != null && param.getLast30DaysAvgSales() != 0.0) {
            request.setLast30DaysAvgSales(param.getLast30DaysAvgSales());
        }

        if (param.getIn7DaysAvgSales() != null && param.getIn7DaysAvgSales() != 0.0) {
            request.setIn7DaysAvgSales(param.getIn7DaysAvgSales());
        }

        if (param.getSampleNum() != 0.0) {
            request.setSampleNum(param.getSampleNum());
        }

        if (param.getPeriodAEstimateVariance() != null && param.getPeriodAEstimateVariance() != 0.0) {
            request.setPeriodAEstimateVariance(param.getPeriodAEstimateVariance());
        }

        if (param.getPeriodBEstimateVariance() != null && param.getPeriodBEstimateVariance() != 0.0) {
            request.setPeriodBEstimateVariance(param.getPeriodBEstimateVariance());
        }

        if (param.getSalesPeriod() != null && param.getSalesPeriod() != 0.0) {
            request.setSalesPeriod(param.getSalesPeriod());
        }

        if (param.getShopId() != null && !param.getShopId().isEmpty()) {
            request.setShopId(param.getShopId());
        }

        if (param.getSafetyStockModel() != null && !param.getSafetyStockModel().isEmpty()) {
            request.setSafetyStockModel(param.getSafetyStockModel());
        }

        if (param.getReplenishModel() != null && !param.getReplenishModel().isEmpty()) {
            request.setReplenishModel(param.getReplenishModel());
        }

        if (param.getOrderableDates() != null && !param.getOrderableDates().isEmpty()) {
            request.setOrderableDates(param.getOrderableDates());
        }

        if (param.getReplAdvancedDate() != null && param.getReplAdvancedDate() != 0) {
            request.setReplAdvancedDate(param.getReplAdvancedDate());
        }

        if (param.getHistory30DaysSalesLength() != null && param.getHistory30DaysSalesLength() != 0) {
            request.setHistory30DaysSalesLength(param.getHistory30DaysSalesLength());
        }


        if (param.getBestSellLabel() != null && param.getBestSellLabel()) {
            request.setBestSellLabel(param.getBestSellLabel());
        }

        if (param.getNodeStock() != null && param.getNodeStock() != 0.0) {
            request.setNodeStock(param.getNodeStock());
        }

        if (param.getIsNew() != null && param.getIsNew()) {
            request.setIsNew(param.getIsNew());
        }

        if (param.getIsShortWarranty() != null && param.getIsShortWarranty()) {
            request.setIsShortWarranty(param.getIsShortWarranty());
        }

        if (param.getIsStandard() != null && param.getIsStandard()) {
            request.setIsStandard(param.getIsStandard());
        }

        if (param.getBranchNodeStock() != null && !param.getBranchNodeStock().isEmpty()) {
            request.setBranchNodeStock(param.getBranchNodeStock());
        }

        if (param.getBranchPeriodASales() != null && !param.getBranchPeriodASales().isEmpty()) {
            request.setBranchPeriodASales(param.getBranchPeriodASales());
        }

        if (param.getBranchPeriodBSales() != null && !param.getBranchPeriodBSales().isEmpty()) {
            request.setBranchPeriodBSales(param.getBranchPeriodBSales());
        }

        if (param.getBranchPeriodAVariance() != null && !param.getBranchPeriodAVariance().isEmpty()) {
            request.setBranchPeriodAVariance(param.getBranchPeriodAVariance());
        }

        if (param.getBranchPeriodBVariance() != null && !param.getBranchPeriodBVariance().isEmpty()) {
            request.setBranchPeriodBVariance(param.getBranchPeriodBVariance());
        }

        if (param.getBranchRequests() != null && !param.getBranchRequests().isEmpty()) {
            request.setBranchRequests(param.getBranchRequests());
        }

        if (param.getLevel() != null && param.getLevel() != 0.0) {
            request.setLevel(param.getLevel());
        }

        if (param.getDate() != null && !param.getDate().isEmpty()) {
            request.setDate(param.getDate());
        }


        if (param.getServiceLevel() == null || param.getServiceLevel() == 0) {
            double serviceLevel = calServiceLevel(request);
            request.setServiceLevel(serviceLevel);
        }

        if (param.getMinServiceLevel() == null || param.getMinServiceLevel() == 0) {
            double minServiceLevel = calMinServiceLevel(request);
            request.setMinServiceLevel(minServiceLevel);
        }


        return request;
    }


}
