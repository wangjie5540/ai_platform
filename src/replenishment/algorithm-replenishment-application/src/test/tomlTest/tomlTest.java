package tomlTest;

import com.digitforce.algorithm.consts.ReplProcessConsts;
import com.digitforce.algorithm.dto.ModelParam;
import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.replenishment.builder.ReplBuilderAdvanced;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;

import java.util.Map;

public class tomlTest {

    public static void main(String[] args) {
//        Map<String, Object> res = new tom().loadMenuOrder("");
//        Map config =  (Map) res.get("default");
//        System.out.println(JSON.toJSON(config.toString()));
//        Map<String, Map<String, Double>> newProdSafetyStockCoef = (Map<String, Map<String, Double>>) config.get("newProdSafetyStockCoef");
//        System.out.println(newProdSafetyStockCoef);
//        new ParseConfig().parseTomlConfig(Double.class, "/config.toml", "residualSafetyStockT", 0.0);
        String expression = ParseReplConfig.parseString(ParseReplConfig.grossDemandKey);

        ReplRequest request = new ReplRequest();
        request.setDms(5.0);
        request.setMinDisplayRequire(100.0);
        request.setPeriodASales(100.0);
        request.setPeriodADays(10.0);
        request.setPeriodAEstimateVariance(15.0);
        request.setSampleNum(100.0);
        request.setIn7DaysAvgSales(3.0);
        request.setLast7DaysAvgSales(2.0);
        request.setLast30DaysAvgSales(1.0);
        request.setServiceLevel(0.95);
        request.setStock(0.0);
        request.setTransferStock(0.0);
        request.setOrderDeliveryDays(0.0);

        ModelParam modelParam = ParseReplConfig.getConfigModelParam();
        /**
         * 获取配置
         */
        Map<String,String> alias = ParseReplConfig.parseMap(ParseReplConfig.aliasKey);
        String periodStrategy = ParseReplConfig.parseString(ParseReplConfig.periodReplFlagKey);
        // 通过配置
        boolean supplenmentStrategyFlag = ParseReplConfig.parseBoolean(ParseReplConfig.supplementStrategyKey);

        ReplBuilderAdvanced builder = new ReplBuilderAdvanced(request, modelParam,null, ReplProcessConsts.singleStagedRepl, alias, periodStrategy, supplenmentStrategyFlag);
        builder.setPeriod("展望期A");
//        Component safetyStock = builder.buildSafetyStock();
//        Component grossDemand = builder.buildGrossDemand();
//
//        grossDemand.addSubComponent(safetyStock);
//        double value = grossDemand.calc();
//        System.out.println(value);

//        ReplenishmentLog rlog = new ReplenishmentLog();
//        rlog.addSubLog("毛需求", grossDemand.getReplLog());
//        rlog.postProcessLogPeriod("2022-06-06", request);
//        rlog.parseLog(false);
    }
}
