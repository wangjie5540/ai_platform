package com.digitforce.algorithm.replenishment.builder;

import com.digitforce.algorithm.consts.ReAllocateStrategy;
import com.digitforce.algorithm.dto.ParentNode;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.replenishment.component.Component;
import com.digitforce.algorithm.replenishment.component.compoundComponent.ReplPriorityReallocate;
import com.digitforce.algorithm.replenishment.component.compoundComponent.ReplRatioReallocate;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;

import java.util.*;
import java.util.stream.Collectors;

public class PostProcessBuilder {
    private List<ReplResult> results;
    private ParentNode parentNode;
    private String reAllocateStratey = ParseReplConfig.parseString(ParseReplConfig.reAllocateStrategyKey);

    public List<Component> buildReAllocateComponent() {
        // 降序
        results = results.stream().sorted(Comparator.comparing(ReplResult::getReplQuant).reversed()).collect(Collectors.toList());
        Map<String, Double> parentStock = parentNode.getParentStock();
        Map<String, Double> totalReplQuant = results.stream().collect(Collectors.groupingBy(c -> c.getGoodsId(), Collectors.summingDouble(ReplResult::getReplQuant)));
        List<Component> components = new ArrayList<>();
        for (ReplResult result:results) {
            String goodsId = result.getGoodsId();
            Component reAllocateComponent;
            if (ReAllocateStrategy.reAllocateByRatio.equals(reAllocateStratey)) {
                reAllocateComponent = new ReplRatioReallocate();
                reAllocateComponent.setVariable("上级库存量", parentStock.getOrDefault(goodsId, 0.0));
                reAllocateComponent.setVariable("总补货量", totalReplQuant.getOrDefault(goodsId, 0.0));
            } else {
                reAllocateComponent = new ReplPriorityReallocate();
            }
            Map<String, Object> param = new HashMap<>();
            param.put("补货结果", result);
            reAllocateComponent.setParameters(param);
            components.add(reAllocateComponent);
        }
        return components;
    }

    public PostProcessBuilder(List<ReplResult> results, ParentNode parentNode) {
        this.results = results;
        this.parentNode = parentNode;
    }
}
