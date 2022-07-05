
package com.digitforce.algorithm.replenishment.component.compoundComponent;

import com.digitforce.algorithm.consts.ReAllocateStrategy;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.replenishment.component.Component;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Getter
public class ReplQuantReAllocate {
    private List<ReplResult> results;
    private Map<String, Double> parentNodeStock;
    private List<Component> components;
    private String reAllocateStratey = ParseReplConfig.parseString(ParseReplConfig.reAllocateStrategyKey);
    private Map<String, Double> parentLeftStock;

    /** 判断是否要重新分配补货量；
     *
     * @return
     */
    private boolean isReallocateOrNot(String skuId) {
        List<ReplResult> resultForSku = results.stream().filter(e->e.getGoodsId().equals(skuId)).collect(Collectors.toList());
        double parentStock = parentNodeStock.getOrDefault(skuId, 0.0);
        double totalReplQuant = resultForSku.stream().mapToDouble(e->e.getReplQuant()).sum();
        if (parentStock < totalReplQuant)
            return  true;
        return false;
    }


    public void reAllocate() {
        for (Component component:components) {
            ReplResult replResult = (ReplResult) component.getParameters().get("补货结果");
            String skuId = replResult.getGoodsId();
            double skuParentLeftStock = parentLeftStock.getOrDefault(skuId, 0.0);
            if (ReAllocateStrategy.reAllocateByPriority.equals(reAllocateStratey)) {
                component.setVariable("父节点剩余库存量", skuParentLeftStock);
            }
            boolean flag = isReallocateOrNot(skuId);
            if (flag) {
                double value = component.calc();
                replResult.setReplQuant(value);
                parentLeftStock.put(skuId, skuParentLeftStock - value);
            } else {
                log.info("父节点库存充足，无需再分配");
            }
        }
    }

    public ReplQuantReAllocate(List<ReplResult> results, Map<String, Double> parentNodeStock, List<Component> components) {
        this.results = results;
        this.parentNodeStock = parentNodeStock;
        this.parentLeftStock = parentNodeStock;
        this.components = components;
    }

}
