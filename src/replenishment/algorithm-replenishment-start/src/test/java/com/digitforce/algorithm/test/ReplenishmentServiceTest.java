package com.digitforce.algorithm.test;

import com.digitforce.algorithm.dto.ModelParam;
import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.replenishment.ReplenishmentService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("dev")
public class ReplenishmentServiceTest {

    @Resource
    private ReplenishmentService replenishmentService;

    @Test
    public void test() throws InstantiationException, IllegalAccessException {
        List<ReplRequest> requestList = new ArrayList<>();
        ReplRequest request = new ReplRequest();
        request.setSkuId("AAAAAAAA");
        request.setSkuProperty("常规品");
        request.setPeriodASales(100.0);
//        request.setPeriodBSales(150);
        request.setPeriodBSales(150.0);
        request.setDms(3.0);
        request.setSafetyStockDays(2.0);
//        request.setMinInStock(3);
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
        requestList.add(request);

        ModelParam modelParam = new ModelParam();
        modelParam.setResidualSafetyStockT(0.3);

        List<ReplResult> results = replenishmentService.processRequest(requestList, modelParam, "2022-05-10", null, 2, true);
    }


}
