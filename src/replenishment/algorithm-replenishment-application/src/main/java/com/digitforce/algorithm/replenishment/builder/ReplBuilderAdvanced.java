package com.digitforce.algorithm.replenishment.builder;

import com.digitforce.algorithm.dto.ModelParam;
import com.digitforce.algorithm.dto.ParentNode;
import com.digitforce.algorithm.consts.ReplProcessConsts;
import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.replenishment.component.Component;
import com.digitforce.algorithm.replenishment.component.compoundComponent.*;
import com.digitforce.algorithm.replenishment.component.unitComponent.*;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public class ReplBuilderAdvanced {
    private String strategy;
    private String period;
    private double serviceLevel;
    private ReplRequest request;
    private ModelParam modelParam;

    /**
     * 父节点信息
     */
    private ParentNode parentNode;

    private Integer replProcess;

    /**
     * 获取配置
     */
    private Map<String,String> alias;

    private String periodStrategy;

    // 通过配置
    private boolean supplenmentStrategyFlag;

    public ReplBuilderAdvanced(ReplRequest request, ModelParam modelParam, ParentNode parentNode, Integer replProcess,
                               Map<String, String> alias, String periodStrategy, boolean supplenmentStrategyFlag) {
        this.request = request;
        this.modelParam = modelParam;
        this.parentNode = parentNode;
        this.replProcess =replProcess;
        this.alias = alias;
        this.periodStrategy = periodStrategy;
        this.supplenmentStrategyFlag = supplenmentStrategyFlag;
    }

    /** 构建一个展望期的组件
     *
     * @param period
     * @param serviceLevel
     * @return
     */
    public Component periodBuild( ReplRequest request, String period, double serviceLevel) {
        this.period = period;
        this.serviceLevel = serviceLevel;
        Component safetyStock = buildSafetyStock(request, serviceLevel);
        Component grossDemand = buildGrossDemand(request);
        Component validStock  = buildStock(request);
        Component basicReplQuant = new BasicReplQuant();

        // 多级补货且配置有补充策略，且有子分支
        if (replProcess.equals(ReplProcessConsts.multiStagedRepl) && !period.contains("B") && supplenmentStrategyFlag &&
                (request.getBranchRequests() != null && !request.getBranchRequests().isEmpty())) {
            basicReplQuant.setExpression("max(max(毛需求 - 有效库存,需求上限), EOQ)");
            // 构建需求上限
            Component supplementBound = buildbasicReplBound(request, serviceLevel);
            basicReplQuant.addSubComponent(supplementBound);
        }
        grossDemand.addSubComponent(safetyStock);
        basicReplQuant.addSubComponent(grossDemand);
        basicReplQuant.addSubComponent(validStock);

        basicReplQuant.setStage( period);

        double replOrNotFlag = period.contains("A") ? 1.0:0.0;
        basicReplQuant.setVariable("判断是否补货", replOrNotFlag);
        return basicReplQuant;
    }


    /** 构建不同展望期组件
     *
     * @param serviceLevel
     * @return
     */
    public Component mutliPeriodBuild(ReplRequest replRequest, double serviceLevel) {
        Component periodAComponent = periodBuild(replRequest,"展望期A", serviceLevel);
        periodAComponent.setName("展望期A净需求");

        Component periodNetDemand = new PeriodNetDemand();
        periodNetDemand.addSubComponent(periodAComponent);

        periodNetDemand.setExpression("max(展望期A净需求, 0)");
        if ("缺货不补".equals(periodStrategy)) {
            Component periodBComponent = periodBuild(replRequest, "展望期B", serviceLevel);
            periodBComponent.setName("展望期B净需求");
            periodNetDemand.addSubComponent(periodBComponent);
        }
        return periodNetDemand;
    }

    /** 构建不同服务水平不同展望期的组件
     *
     * @return
     */
    public Component build() {
        Component serviceLevelComponent = mutliPeriodBuild(request, request.getServiceLevel());
        serviceLevelComponent.setName("期望订货量");
        serviceLevelComponent.setStage("期望服务水平");
        Component minServiceLevelComponent = mutliPeriodBuild(request, request.getMinServiceLevel());
        minServiceLevelComponent.setName("最小订货量");
        minServiceLevelComponent.setStage("最小服务水平");
        // 最佳补货量
        Component bestReplQunat = new AdjustBestReplQuant();
        bestReplQunat = new ParseValueByAlias(alias, request, period, modelParam).parseValueByAlias(bestReplQunat);
        bestReplQunat.addSubComponent(serviceLevelComponent);
        bestReplQunat.addSubComponent(minServiceLevelComponent);
        return bestReplQunat;
    }


    /** 构造安全库存
     *
     * @return
     */
    private Component buildSafetyStock(ReplRequest request, double serviceLevel) {
        Component safetyStock;
        Component safetyStockBound = null;

        // 残差安全库存
        if (request.getPeriodAEstimateVariance() != 0.0 || request.getPeriodBEstimateVariance() != 0.0) {
            safetyStock =  new ResidualSafetyStock();
            safetyStock =  new ParseValueByAlias(alias, request, period, modelParam).parseValueByAlias(safetyStock);
            safetyStock.setVariable("服务水平", serviceLevel);

            // 单级补货
            if (replProcess.equals(ReplProcessConsts.singleStagedRepl) || (request.getBranchRequests() == null && !request.getBranchRequests().isEmpty())) {
                safetyStockBound = new SafetyStockBound();
                safetyStockBound = new ParseValueByAlias(alias, request, period, modelParam).parseValueByAlias(safetyStockBound);
                safetyStockBound.setVariable("展望期天数", request.getPeriodADays());
            } else {
                // 多级补货
                safetyStockBound = new MultiStagedSafetyStockBound();
                List<Component> branchSafetyStock = buildMultiStagedSafetyStockBound(request.getBranchRequests());
                safetyStockBound.setParameter("链路安全库存下界", branchSafetyStock);
            }

            // 抽样安全库存
        } else if (request.getPercentile() != 0) {
            safetyStock = new BoostrapSafetyStock();
            safetyStock =  new ParseValueByAlias(alias, request, period, modelParam).parseValueByAlias(safetyStock);
            // 新品安全库存
        } else {
            safetyStock = new NewGoodsafetyStock();
            safetyStock =  new ParseValueByAlias(alias, request, period, modelParam).parseValueByAlias(safetyStock);
        }

        if (safetyStockBound != null) {

            safetyStock.addSubComponent(safetyStockBound);
        }
        return  safetyStock;
    }


    private Component buildGrossDemand(ReplRequest request) {
        String expression = ParseReplConfig.parseString(ParseReplConfig.grossDemandKey);
        Component grossDemand;
        // 如果配置了毛需求公式，则读取配置
        if (expression == null || expression.isEmpty())
            grossDemand = new GrossDemand();
        else
            grossDemand = new GrossDemand(expression);

        grossDemand = new ParseValueByAlias(alias, request, period, modelParam).parseValueByAlias(grossDemand);

        setGrossDemandVariable(request, grossDemand);
        return grossDemand;
    }


    private void setGrossDemandVariable(ReplRequest request, Component grossDemand) {
        if (request.getIsShortWarranty()) {
            grossDemand.setVariable("需求下限", request.getLast30DaysAvgSales() * request.getPeriodADays());
        } else if (request.getPercentile() != 0) {
            grossDemand.setVariable("销量期望", 0.0);
        }
    }

    /** 构建有效库存组件
     *
     * @return
     */
    private Component buildStock(ReplRequest request) {
        Component validStock;
        // 单级补货
        if (replProcess.equals(ReplProcessConsts.singleStagedRepl)) {
            validStock = new ValidStock();
        } else {
            // 多级补货
            validStock = new MultiStagedValidStock();

        }
        if (period.contains("A")) {
            validStock = new ParseValueByAlias(alias, request, period, modelParam).parseValueByAlias(validStock);
            // 日清
            if (request.getSalesPeriod() == 1.0) {
                validStock.setVariable("当前库存", 0.0);
                validStock.setVariable("调拨在途", 0.0);
            }
        }

        return validStock;
    }


    /** 构建每个子节点的安全库存上限
     *
     * @param branchRequests
     * @return
     */
    private List<Component> buildMultiStagedSafetyStockBound(Map<String, ReplRequest> branchRequests) {
        List<Component> safetyStockBounds = new ArrayList<>();
        for (Map.Entry<String, ReplRequest> entry:branchRequests.entrySet()) {
            ReplRequest request = entry.getValue();
            Component safetyStockBound = new SafetyStockBound();
            safetyStockBound =  new ParseValueByAlias(alias, request, period, modelParam).parseValueByAlias(safetyStockBound);
            safetyStockBound.setVariable("展望期天数", request.getPeriodADays());
            safetyStockBounds.add(safetyStockBound);
        }
        return safetyStockBounds;
    }

    /** 多级补充策略：每个链路下的基础补货量
     *
     * @return
     */
    private List<Component> residualSupplementElementNew(double serviceLevel) {
        List<Component> componentList = new ArrayList<>();
        Map<String, ReplRequest> branchRequests = request.getBranchRequests();
        int root = 1;
        for (Map.Entry<String, ReplRequest> entry:branchRequests.entrySet()) {
            ReplRequest sub = entry.getValue();
            if (sub != null) {
                Component supplementComponent = periodBuild(sub,period, serviceLevel);
                String updatedStage = supplementComponent.getStage().isEmpty()?"链路"+root:"链路"+root+supplementComponent.getStage();
                supplementComponent.setStage(updatedStage);
                componentList.add(supplementComponent);
                root +=1;
            }

        }
        return componentList;
    }

    /** 构建补货量上限
     *
     * @return
     */
    private Component buildbasicReplBound(ReplRequest request, double serviceLevel) {
        Component validStock = buildStock(request);
        Component basicReplBound = new BasicReplQuantUpperBound();
        Map<String, Object> parameters = new HashMap<>();
        List<Component> components = residualSupplementElementNew( serviceLevel);
        parameters.put("链路补货量列表", components);
        parameters.put("有效库存", validStock);
        basicReplBound.setParameters(parameters);
        return basicReplBound;
    }

}
