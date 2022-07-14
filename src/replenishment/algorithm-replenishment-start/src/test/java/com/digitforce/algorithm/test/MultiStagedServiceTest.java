package com.digitforce.algorithm.test;

import com.digitforce.algorithm.dto.ModelParam;
import com.digitforce.algorithm.dto.ParentNode;
import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.replenishment.ReplenishmentService;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("dev")
public class MultiStagedServiceTest {


    @Autowired
    ReplenishmentService replenishmentService;


    @Test
    public void test() {
        List<ReplRequest> requestList = new ArrayList<>();
        // 假设是节点5
        //              5
        //             / \
        //            1   2
        // parentNode是7
        ReplRequest request0 = new ReplRequest();
        request0.setDefaultValue();
        request0.setPeriodASales(1000.0);
        request0.setPeriodBSales(150.0);
        request0.setPeriodAEstimateVariance(10.0);
        request0.setPeriodBEstimateVariance(15.0);
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
        request0.setNodeStock(20.0);
        ReplRequest sub1 = new ReplRequest();
        sub1.setDefaultValue();
        sub1.setStock(5.0);
        sub1.setPeriodASales(300.0);
        sub1.setPeriodAEstimateVariance(8.0);
        sub1.setPeriodADays(3.0);
        sub1.setTransferStock(0.0);
        sub1.setLast7DaysAvgSales(1.0);
        sub1.setLast30DaysAvgSales(2.9);
        sub1.setIn7DaysAvgSales(3.0);
        ReplRequest sub2 = new ReplRequest();
        sub2.setDefaultValue();
        sub2.setStock(15.0);
        sub2.setPeriodASales(700.0);
        sub2.setPeriodAEstimateVariance(10.0);
        sub2.setPeriodADays(3.0);
        sub2.setTransferStock(0.0);
        sub2.setLast7DaysAvgSales(2.3);
        sub2.setLast30DaysAvgSales(2.5);
        sub2.setIn7DaysAvgSales(1.2);
        Map<String, ReplRequest> branchRequests = new HashMap<>();
        branchRequests.put("1", sub1);
        branchRequests.put("2", sub2);

        request0.setBranchRequests(branchRequests);
        request0.setSkuId("request0");

        ParentNode parentNode = new ParentNode();
        parentNode.setParent("7");
        Map<String, Double> parentStock = new HashMap<>();
        parentStock.put("request0", 199.0);
        parentNode.setParentStock(parentStock);

        ModelParam modelParam = ParseReplConfig.getConfigModelParam();
        String date = "2022-05-17";
        requestList.add(request0);
        List<ReplResult> results = replenishmentService.processRequest(requestList, modelParam, date, parentNode, 2,true);

    }
}
