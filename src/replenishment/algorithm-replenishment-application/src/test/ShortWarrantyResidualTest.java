import com.digitforce.algorithm.dto.ModelParam;
import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.dto.ReplenishmentLog;
import com.digitforce.algorithm.dto.data.PreprocessData;
import com.digitforce.algorithm.replenishment.builder.ReplBuilderAdvanced;
import com.digitforce.algorithm.replenishment.component.Component;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import org.bouncycastle.cert.ocsp.Req;
import org.junit.Test;

import java.util.Map;

public class ShortWarrantyResidualTest {

    @Test
    public void test() throws InstantiationException, IllegalAccessException {
        ReplRequest request = new ReplRequest();
        request.setPeriodASales(100.0);
        request.setPeriodBSales(150.0);
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
        request.setPeriodAEstimateVariance(20.0);
        request.setPeriodBEstimateVariance(15.0);
        request.setIsShortWarranty(true);

        ReplRequest r = new ReplRequest();
        r.setDefaultValue();
        r = PreprocessData.processReplRequest(request);
        ModelParam modelParam = ParseReplConfig.getConfigModelParam();
        /**
         * 获取配置
         */
        Map<String,String> alias = ParseReplConfig.parseMap(ParseReplConfig.aliasKey);
        String periodStrategy = ParseReplConfig.parseString(ParseReplConfig.periodReplFlagKey);
        // 通过配置
        boolean supplenmentStrategyFlag = ParseReplConfig.parseBoolean(ParseReplConfig.supplementStrategyKey);

        ReplBuilderAdvanced builder = new ReplBuilderAdvanced(r, modelParam, null,1, alias, periodStrategy, supplenmentStrategyFlag);

        Component component =  builder.build();
        double value = component.calc();
        System.out.println("value:" + value);
        ReplenishmentLog log = new ReplenishmentLog();
        log.addSubLog("补货量", component.getReplLog());
        log.postProcessLogPeriod("2022-06-17", r);
        log.parseLog(request.getSkuId(), request.getShopId(), false);
        System.out.println("value:"+ value);
    }
}
