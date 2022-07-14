package com.digitforce.algorithm.replenishment;

import com.alibaba.fastjson.JSON;
import com.digitforce.algorithm.dto.*;
import com.digitforce.algorithm.dto.data.PreprocessData;
import com.digitforce.algorithm.replenishment.builder.PostProcessBuilder;
import com.digitforce.algorithm.replenishment.builder.ReplBuilderAdvanced;
import com.digitforce.algorithm.replenishment.component.Component;
import com.digitforce.algorithm.replenishment.component.compoundComponent.ReplQuantReAllocate;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@Service
public class ReplenishmentService {

    /**
     * 获取配置
     */
    Map<String,String> alias = ParseReplConfig.parseMap(ParseReplConfig.aliasKey);

    private String periodStrategy = ParseReplConfig.parseString(ParseReplConfig.periodReplFlagKey);

    // 通过配置
    private boolean supplenmentStrategyFlag = ParseReplConfig.parseBoolean(ParseReplConfig.supplementStrategyKey);

    public List<ReplResult> processRequest(List<ReplRequest> replRequestList, ModelParam modelParam, String date, ParentNode parentNode, Integer replProcess, boolean logFlag)  {
        List<ReplResult> results = new ArrayList<>();

        log.info("商品参数列表长度:{}", replRequestList.size());
        log.info("模型参数:{}", JSON.toJSONString(modelParam));
        for (ReplRequest request:replRequestList) {
            try {
                log.info("商品参数:{}", JSON.toJSONString(request));
                request = PreprocessData.processReplRequest(request);
                log.info("处理后商品参数:{}", JSON.toJSONString(request));
                log.info("开始:{}补货计算", request.getSkuId());
                ReplBuilderAdvanced builder = new ReplBuilderAdvanced(request, modelParam, parentNode, replProcess,alias, periodStrategy, supplenmentStrategyFlag);
                Component component = builder.build();
                double replenishmentResult = component.calc();

                // 结果处理
                log.info("结束:{}补货计算, 补货量:{}", request.getSkuId(), replenishmentResult);

                ReplResult replResult = new ReplResult(replenishmentResult, builder.getStrategy(), request.getShopId(), request.getSkuId());
                ReplenishmentLog rlog = processLog(component.getReplLog(), date, request);
                Map<String, String> middleResMap = ParseReplConfig.parseMap(ParseReplConfig.middleResMapKey);
                Map<String, Double> middleRes = rlog.processMidResult(middleResMap, new HashMap<>());
                if (logFlag) {
                    replResult.setReplenishmentLog(rlog);
                }
                replResult.setMiddleRes(middleRes);
                log.info("安全库存:{}; shopId:{}", replResult.getMiddleRes(), replResult.getShopId());
                results.add(replResult);

            } catch (Exception e) {
                log.error("商品:{}计算补货量报错:{}", request.getSkuId(), e);
            }
        }


        if (parentNode != null) {
            // 多级补货
            List<Component> reAllocateComponent = new PostProcessBuilder(results, parentNode).buildReAllocateComponent();
            ReplQuantReAllocate allocation = new ReplQuantReAllocate(results, parentNode.getParentStock(), reAllocateComponent);
            allocation.reAllocate();
            results = allocation.getResults();
        }
        return  results;
    }


    public ReplenishmentLog processLog(ReplenishmentLog sublog, String date, ReplRequest request) {
        // 解析补货日志
        ReplenishmentLog rlog = new ReplenishmentLog();
        rlog.addSubLog("补货量", sublog);
        rlog.postProcessLogPeriod(date, request);
        //TODO：本地测试用
        rlog.parseLog(request.getSkuId(), request.getShopId(), true);
        return rlog;
    }


}
