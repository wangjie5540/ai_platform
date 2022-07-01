import com.digitforce.algorithm.dto.ModelParam;
import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.dto.ReplenishmentLog;
import com.digitforce.algorithm.dto.data.PreprocessData;
import com.digitforce.algorithm.replenishment.builder.ReplBuilderAdvanced;
import com.digitforce.algorithm.replenishment.component.Component;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;


import java.util.*;

@Slf4j
public class NewProdSafetyStockTest {

    @Test
    public void test() {
        List<ReplRequest> requestList = new ArrayList<>();

        ReplRequest request = new ReplRequest();
        request.setPeriodASales(150.0);
        request.setPeriodBSales(100.0);
        request.setDms(3.0);
        request.setSafetyStockDays(2.0);
        request.setMinInStock(0.0);
        request.setMinDisplayRequire(10.0);
        request.setMinOrderQ(12.0);
        request.setStock(50.0);
        request.setTransferStock(10.0);
        request.setUnit(12.0);
        request.setPeriodADays(10.0);
        request.setPeriodBDays(6.0);
        request.setServiceLevel(0.95);
        request.setMinServiceLevel(0.85);
        request.setLast7DaysAvgSales(5.0);
        request.setLast30DaysAvgSales(7.0);
        request.setIn7DaysAvgSales(6.0);
        request.setPeriodDaysVariance(2.0);
        request.setSampleNum(100.0);
        request.setMinOrderQ(40.0);
        request.setUnit(10.0);
        request.setWarrantyPeriods(200.0);
        request.setSalesPeriod(8.0);
        request.setOrderDeliveryDays(4.0);
        request.setNodeStock(20.0);


        ReplRequest request1 = new ReplRequest();
        request1.setPeriodASales(150.0);
        request1.setPeriodBSales(100.0);
        request1.setDms(3.0);
        request1.setSafetyStockDays(2.0);
        request1.setMinInStock(0.0);
        request1.setMinDisplayRequire(10.0);
        request1.setMinOrderQ(12.0);
        request1.setStock(50.0);
        request1.setTransferStock(10.0);
        request1.setUnit(12.0);
        request1.setPeriodADays(10.0);
        request1.setPeriodBDays(6.0);
        request1.setServiceLevel(0.95);
        request1.setMinServiceLevel(0.85);
        request1.setLast7DaysAvgSales(5.0);
        request1.setLast30DaysAvgSales(7.0);
        request1.setIn7DaysAvgSales(6.0);
        request1.setPeriodDaysVariance(2.0);
        request1.setSampleNum(100.0);
        request1.setMinOrderQ(40.0);
        request1.setUnit(10.0);
        request1.setWarrantyPeriods(100.0);
        request1.setSalesPeriod(8.0);
        request1.setOrderDeliveryDays(4.0);

        ReplRequest request2 = new ReplRequest();
        request2.setPeriodASales(150.0);
        request2.setPeriodBSales(100.0);
        request2.setDms(3.0);
        request2.setSafetyStockDays(2.0);
        request2.setMinInStock(0.0);
        request2.setMinDisplayRequire(10.0);
        request2.setMinOrderQ(12.0);
        request2.setStock(50.0);
        request2.setTransferStock(10.0);
        request2.setUnit(12.0);
        request2.setPeriodADays(10.0);
        request2.setPeriodBDays(6.0);
        request2.setServiceLevel(0.95);
        request2.setMinServiceLevel(0.85);
        request2.setLast7DaysAvgSales(5.0);
        request2.setLast30DaysAvgSales(7.0);
        request2.setIn7DaysAvgSales(6.0);
        request2.setPeriodDaysVariance(2.0);
        request2.setSampleNum(100.0);
        request2.setMinOrderQ(40.0);
        request2.setUnit(10.0);
        request2.setWarrantyPeriods(70.0);
        request2.setSalesPeriod(20.0);
        request2.setOrderDeliveryDays(4.0);


        ReplRequest request3 = new ReplRequest();
        request3.setPeriodASales(150.0);
        request3.setPeriodBSales(100.0);
        request3.setDms(3.0);
        request3.setSafetyStockDays(2.0);
        request3.setMinInStock(0.0);
        request3.setMinDisplayRequire(10.0);
        request3.setMinOrderQ(12.0);
        request3.setStock(50.0);
        request3.setTransferStock(10.0);
        request3.setUnit(12.0);
        request3.setPeriodADays(10.0);
        request3.setPeriodBDays(6.0);
        request3.setServiceLevel(0.95);
        request3.setMinServiceLevel(0.85);
        request3.setLast7DaysAvgSales(5.0);
        request3.setLast30DaysAvgSales(7.0);
        request3.setIn7DaysAvgSales(6.0);
        request3.setPeriodDaysVariance(2.0);
        request3.setSampleNum(100.0);
        request3.setMinOrderQ(40.0);
        request3.setUnit(10.0);
        request3.setWarrantyPeriods(70.0);
        request3.setSalesPeriod(20.0);
        request3.setOrderDeliveryDays(4.0);


        ReplRequest request4 = new ReplRequest();
        request4.setPeriodASales(150.0);
        request4.setPeriodBSales(100.0);
        request4.setDms(3.0);
        request4.setSafetyStockDays(2.0);
        request4.setMinInStock(0.0);
        request4.setMinDisplayRequire(10.0);
        request4.setMinOrderQ(12.0);
        request4.setStock(50.0);
        request4.setTransferStock(10.0);
        request4.setUnit(12.0);
        request4.setPeriodADays(10.0);
        request4.setPeriodBDays(6.0);
        request4.setServiceLevel(0.95);
        request4.setMinServiceLevel(0.85);
        request4.setLast7DaysAvgSales(5.0);
        request4.setLast30DaysAvgSales(7.0);
        request4.setIn7DaysAvgSales(6.0);
        request4.setPeriodDaysVariance(2.0);
        request4.setSampleNum(100.0);
        request4.setMinOrderQ(40.0);
        request4.setUnit(10.0);
        request4.setWarrantyPeriods(70.0);
        request4.setSalesPeriod(8.0);
        request4.setOrderDeliveryDays(4.0);


        ReplRequest request5 = new ReplRequest();
        request5.setPeriodASales(150.0);
        request5.setPeriodBSales(100.0);
        request5.setDms(3.0);
        request5.setSafetyStockDays(2.0);
        request5.setMinInStock(0.0);
        request5.setMinDisplayRequire(10.0);
        request5.setMinOrderQ(12.0);
        request5.setStock(50.0);
        request5.setTransferStock(10.0);
        request5.setUnit(12.0);
        request5.setPeriodADays(10.0);
        request5.setPeriodBDays(6.0);
        request5.setServiceLevel(0.95);
        request5.setMinServiceLevel(0.85);
        request5.setLast7DaysAvgSales(5.0);
        request5.setLast30DaysAvgSales(7.0);
        request5.setIn7DaysAvgSales(6.0);
        request5.setPeriodDaysVariance(2.0);
        request5.setSampleNum(100.0);
        request5.setMinOrderQ(40.0);
        request5.setUnit(10.0);
        request5.setWarrantyPeriods(70.0);
        request5.setSalesPeriod(1.0);
        request4.setOrderDeliveryDays(4.0);

       requestList = new ArrayList<>(Arrays.asList(request, request1, request2, request3, request4, request5));

        ModelParam modelParam = ParseReplConfig.getConfigModelParam();
        /**
         * 获取配置
         */
        Map<String,String> alias = ParseReplConfig.parseMap(ParseReplConfig.aliasKey);
        String periodStrategy = ParseReplConfig.parseString(ParseReplConfig.periodReplFlagKey);
        // 通过配置
        boolean supplenmentStrategyFlag = ParseReplConfig.parseBoolean(ParseReplConfig.supplementStrategyKey);

        for (ReplRequest replRequest:requestList) {
            replRequest = PreprocessData.processReplRequest(replRequest);
            ReplBuilderAdvanced builder = new ReplBuilderAdvanced(replRequest, modelParam, null, 2, alias, periodStrategy, supplenmentStrategyFlag);
            builder.setModelParam(modelParam);
            Component component = builder.build();
            double value = component.calc();

            ReplenishmentLog log = new ReplenishmentLog();
            log.addSubLog("补货量", component.getReplLog());
            log.postProcessLogPeriod("2022-06-07", replRequest);
            log.parseLog(replRequest.getSkuId(), replRequest.getShopId(), false);
            System.out.println("value:" + value);
        }
    }

}
