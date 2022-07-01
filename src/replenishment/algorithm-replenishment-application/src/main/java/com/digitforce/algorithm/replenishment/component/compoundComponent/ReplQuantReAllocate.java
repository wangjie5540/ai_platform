
package com.digitforce.algorithm.replenishment.component.compoundComponent;

import com.digitforce.algorithm.consts.ReAllocateStrategy;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.replenishment.component.Component;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;

import javax.print.attribute.standard.PresentationDirection;
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

    //    public List<ReplResult> checkAndReallocate() {
//        Set<String> skuSet = results.stream().map(e->e.getGoodsId()).distinct().collect(Collectors.toSet());
//        for (String sku:skuSet) {
//            double skuReplQuant = results.stream().filter(e->e.getGoodsId().equals(sku)).mapToDouble(ee->ee.getReplQuant()).sum();
//            double parentStock = parentNodeStock.getOrDefault(sku, 0.0);
//            // 补货量>父节点
//            if (skuReplQuant > parentStock) {
//                // 重新分配;
//                components.entrySet().stream().filter(e->e.getKey().contains(sku)).collect(Collectors.toSet());
//            }
//
//
//        }
//
////        Map<String, Double> skuReplQuant = results.stream().collect(Collectors.groupingBy(c -> c.getGoodsId(), Collectors.summingDouble(ReplResult::getReplQuant)));
////        for (Map.Entry<String, Double> entry:skuReplQuant.entrySet()) {
////            String skuId = entry.getKey();
////            double totalQuant = entry.getValue();
////            double parentStock = parentNodeStock.getOrDefault(skuId, 0.0);
////            // 补货量 > 父节点库存
////            if (totalQuant - parentStock > 0) {
////
////            }
////        }
//    }
}
