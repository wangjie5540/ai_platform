package com.digitforce.algorithm.replenishment.util;

import com.digitforce.algorithm.dto.ModelParam;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ParseReplConfig {
    public static String newProdSafetyStockCoefKey = "newProdSafetyStockCoef";
    public static String residualSafetyStockTKey = "residualSafetyStockT";
    public static String residualSafetyStockBoundCoefKey = "residualSafetyStockBoundCoef";
    public static String grossDemandKey = "grossDemand";
    public static String aliasKey = "alias";
    public static String periodReplFlagKey = "periodReplStrategy";
    public static String supplementStrategyKey = "multiStagedSupplementStrategy";
    public static String middleResMapKey = "middleResMap";
    public static String reAllocateStrategyKey = "reAllocateStrategy";
    public static String orderableDatesOrderKey = "orderableDatesOrder";

    private static String path = "/config.toml";

    public static String parseString(String key) {
        String expression = new ParseConfig().parseTomlConfig(String.class, path, key, "");
        return expression;
    }

    public static Map<String, String> parseMap(String key) {
        Map<String, String> aliasMap = new ParseConfig().parseTomlConfig(Map.class, path, key, new HashMap());
        aliasMap = aliasMap.entrySet().stream().collect(Collectors.toMap(e->e.getKey().substring(1, e.getKey().length()-1), e->e.getValue()));
        return aliasMap;
    }

    public static Map<String, Map<String, Double>> parseMapMap(String key) {
        Map<String, Map<String, Double>> value = new ParseConfig().parseTomlConfig(Map.class, path, key, new HashMap());
        value = value.entrySet().stream().collect(Collectors.toMap(e->e.getKey().substring(1, e.getKey().length()-1), e->e.getValue().entrySet().stream().collect(Collectors.toMap(ee->ee.getKey().substring(1,ee.getKey().length()-1), ee->ee.getValue()))));
        return value;
    }


    public static boolean parseBoolean(String key) {
        boolean value = new ParseConfig().parseTomlConfig(Boolean.class, path, key, false);
        return value;
    }

    public static List<Integer> parseList (String key) {
        List<Integer> value = new ParseConfig().parseTomlConfig(List.class, path, key, new ArrayList());
        return value;
    }


    /** 生成ModelParam对象
     *
     * @return
     */
    //  TODO:check是否应该放到这儿
    public static ModelParam getConfigModelParam() {
        ParseConfig parseConfig = new ParseConfig();
        Map<String,Map<String, Double>> newProdSafetyStockCoef = parseMapMap(newProdSafetyStockCoefKey);
        double residualSafetyStockT = parseConfig.parseTomlConfig(Double.class, path, residualSafetyStockTKey, 0.0);
        double residualSafetyStockBound = parseConfig.parseTomlConfig(Double.class, path, residualSafetyStockBoundCoefKey, 0.0);
        ModelParam modelParam = new ModelParam();
        modelParam.setNewProdSafetyStockCoef(newProdSafetyStockCoef);
        modelParam.setResidualSafetySotckBoundCoef(residualSafetyStockBound);
        modelParam.setResidualSafetyStockT(residualSafetyStockT);
        return modelParam;
    }

}
