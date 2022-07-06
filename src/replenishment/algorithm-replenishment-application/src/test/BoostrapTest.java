import com.digitforce.algorithm.dto.ModelParam;
import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.dto.ReplenishmentLog;
import com.digitforce.algorithm.replenishment.builder.ReplBuilderAdvanced;
import com.digitforce.algorithm.replenishment.component.Component;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BoostrapTest {

    @Test
    public void test() {
        List<ReplRequest> requests = new ArrayList<>();
        ReplRequest request0 = new ReplRequest();
        request0.setDefaultValue();
        request0.setPeriodASales(100.0);
        request0.setPeriodBSales(150.0);
        request0.setDms(30.0);
        request0.setSafetyStockDays(2.0);
        request0.setMinInStock(0.0);
        request0.setMinDisplayRequire(10.0);
        request0.setMinOrderQ(12.0);
        request0.setStock(50.0);
        request0.setTransferStock(10.0);
        request0.setPeriodADays(3.0);
        request0.setPeriodBDays(2.0);
        request0.setServiceLevel(0.95);
        request0.setMinServiceLevel(0.85);
        request0.setLast7DaysAvgSales(5.0);
        request0.setLast30DaysAvgSales(7.0);
        request0.setIn7DaysAvgSales(6.0);
        request0.setMinOrderQ(40.0);
        request0.setUnit(10.0);
        request0.setPercentile(0.3);
        List<Double> historySales = new ArrayList(Arrays.asList(20.0,22.0,23.0,25.0,22.0,26.0,27.0,29.0,30.0));
        request0.setHistory30DaysSales(historySales);
        request0.setHistory30DaysSalesLength(historySales.size());
        request0.setOrderDeliveryDays(1.0);
        request0.setNodeStock(10.0);
        requests.add(request0);


        ReplRequest request1 = new ReplRequest();
        request1.setDefaultValue();
        request1.setPeriodASales(100.0);
        request1.setPeriodBSales(150.0);
        request1.setDms(30.0);
        request1.setSafetyStockDays(2.0);
        request1.setMinInStock(0.0);
        request1.setMinDisplayRequire(10.0);
        request1.setMinOrderQ(12.0);
        request1.setStock(50.0);
        request1.setTransferStock(10.0);
        request1.setPeriodADays(3.0);
        request1.setPeriodBDays(2.0);
        request1.setServiceLevel(0.95);
        request1.setMinServiceLevel(0.85);
        request1.setLast7DaysAvgSales(5.0);
        request1.setLast30DaysAvgSales(7.0);
        request1.setIn7DaysAvgSales(6.0);
        request1.setMinOrderQ(40.0);
        request1.setUnit(10.0);
        request1.setPercentile(0.3);
        List<Double> historySales1 = new ArrayList();
        request1.setHistory30DaysSales(historySales1);
        request1.setHistory30DaysSalesLength(historySales1.size());
        request1.setOrderDeliveryDays(1.0);
        request1.setMinInStock(220.0);
        requests.add(request1);



        ReplRequest request2 = new ReplRequest();
        request2.setDefaultValue();
        request2.setPeriodASales(100.0);
        request2.setPeriodBSales(150.0);
        request2.setDms(30.0);
        request2.setMinInStock(0.0);
        request2.setMinDisplayRequire(10.0);
        request2.setMinOrderQ(12.0);
        request2.setStock(50.0);
        request2.setTransferStock(10.0);
        request2.setPeriodADays(12.0);
        request2.setPeriodBDays(10.0);
        request2.setServiceLevel(0.95);
        request2.setMinServiceLevel(0.85);
        request2.setLast7DaysAvgSales(5.0);
        request2.setLast30DaysAvgSales(7.0);
        request2.setIn7DaysAvgSales(6.0);
        request2.setMinOrderQ(40.0);
        request2.setUnit(10.0);
        request2.setPercentile(0.3);
        List<Double> historySales2 = new ArrayList(Arrays.asList(20.0,22.0,23.0,25.0,22.0,26.0,27.0,29.0,30.0));
        request2.setHistory30DaysSales(historySales2);
        request2.setHistory30DaysSalesLength(historySales2.size());
        request2.setOrderDeliveryDays(2.0);
        requests.add(request2);

        /**
         * 获取配置
         */
        Map<String,String> alias = ParseReplConfig.parseMap(ParseReplConfig.aliasKey);

        String periodStrategy = ParseReplConfig.parseString(ParseReplConfig.periodReplFlagKey);

        // 通过配置
        boolean supplenmentStrategyFlag = ParseReplConfig.parseBoolean(ParseReplConfig.supplementStrategyKey);

        for (ReplRequest request:requests) {
            ModelParam modelParam = ParseReplConfig.getConfigModelParam();
            ReplBuilderAdvanced builder = new ReplBuilderAdvanced(request, modelParam, null,2, alias, periodStrategy, supplenmentStrategyFlag);
//        Component safetyStock = builder.buildSafetyStock();
//        Component grossDemand = builder.buildGrossDemand();
//        grossDemand.addSubComponent(safetyStock);
//
//        double value = grossDemand.calc();

            Component component = builder.build();
            double value = component.calc();
            System.out.println("value:" + value);
            ReplenishmentLog log = new ReplenishmentLog();
            log.addSubLog("补货量", component.getReplLog());
            log.postProcessLogPeriod("2022-06-17", request);
            log.parseLog(request.getSkuId(), request.getShopId(), false);
            System.out.println("value:" + value);
        }
    }
}
