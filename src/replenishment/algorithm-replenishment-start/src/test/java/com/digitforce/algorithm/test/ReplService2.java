package com.digitforce.algorithm.test;


import com.digitforce.algorithm.dto.*;
import com.digitforce.algorithm.replenishment.ReplenishmentService;
import com.digitforce.algorithm.replenishment.builder.ReplBuilderAdvanced;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("dev")
public class ReplService2 {

    @Autowired
    ReplenishmentService replenishmentService;

    @Test
    public void test() {
        List<ReplRequest> requestList = new ArrayList<>();
//        ReplRequest request = new ReplRequest();
//        request.setPeriodASales(200);
//        request.setPeriodBSales(140);
//        request.setDms(3);
//        request.setSafetyStockDays(2);
//        request.setMinDisplayRequire(10);
//        request.setMinOrderQ(12);
//        request.setStock(50);
//        request.setTransferStock(10);
//        request.setUnit(10);
//        request.setPeriodADays(10);
//        request.setPeriodBDays(6);
//        request.setOrderDeliveryDays(4);
//        request.setLast7DaysAvgSales(5);
//        request.setLast30DaysAvgSales(7);
//        request.setIn7DaysAvgSales(6);
//        request.setPeriodDaysVariance(0);
//        request.setSampleNum(100);
//        request.setMinOrderQ(40);
//        request.setSalesPeriod(4);
//        request.setSkuProperty("常规品");
//        request.setPeriodAEstimateVariance(20);
//        request.setPeriodBEstimateVariance(15);
//        request.setSkuId("sku1");
//        request.setSafetyStockModel("residual");
//        request.setReplenishModel("常规");
//        requestList.add(request);
//
//        ReplRequest request1 = new ReplRequest();
//        request1.setPeriodASales(200);
//        request1.setPeriodBSales(140);
//        request1.setDms(3);
//        request1.setSafetyStockDays(2);
//        request1.setMinDisplayRequire(10);
//        request1.setMinOrderQ(12);
//        request1.setStock(150);
//        request1.setTransferStock(30);
//        request1.setUnit(10);
//        request1.setPeriodADays(10);
//        request1.setPeriodBDays(6);
//        request1.setOrderDeliveryDays(4);
//        request1.setLast7DaysAvgSales(5);
//        request1.setLast30DaysAvgSales(7);
//        request1.setIn7DaysAvgSales(6);
//        request1.setPeriodDaysVariance(0);
//        request1.setSampleNum(100);
//        request1.setMinOrderQ(40);
//        request1.setSalesPeriod(4);
//        request1.setSkuProperty("常规品");
//        request1.setPeriodAEstimateVariance(20);
//        request1.setPeriodBEstimateVariance(15);
//        request1.setSkuId("sku2");
//        request1.setSafetyStockModel("residual");
//        request1.setReplenishModel("常规");
//        requestList.add(request1);
//
//
//        ReplRequest request2 = new ReplRequest();
//        request2.setPeriodASales(200);
//        request2.setPeriodBSales(140);
//        request2.setDms(3);
//        request2.setSafetyStockDays(2);
//        request2.setMinDisplayRequire(10);
//        request2.setMinOrderQ(12);
//        request2.setStock(240);
//        request2.setTransferStock(30);
//        request2.setUnit(10);
//        request2.setPeriodADays(10);
//        request2.setPeriodBDays(6);
//        request2.setOrderDeliveryDays(4);
//        request2.setLast7DaysAvgSales(5);
//        request2.setLast30DaysAvgSales(7);
//        request2.setIn7DaysAvgSales(6);
//        request2.setPeriodDaysVariance(0);
//        request2.setSampleNum(100);
//        request2.setMinOrderQ(40);
//        request2.setSalesPeriod(4);
//        request2.setSkuProperty("常规品");
//        request2.setPeriodAEstimateVariance(20);
//        request2.setPeriodBEstimateVariance(15);
//        request2.setSkuId("sku3");
//        request2.setSafetyStockModel("residual");
//        request2.setReplenishModel("常规");
//        requestList.add(request2);

//        ReplRequest request3= new ReplRequest();
//        request3.setPeriodASales(200);
//        request3.setPeriodBSales(140);
//        request3.setDms(3);
//        request3.setSafetyStockDays(2);
//        request3.setMinDisplayRequire(10);
//        request3.setMinOrderQ(12);
//        request3.setStock(100);
//        request3.setTransferStock(50);
//        request3.setUnit(10);
//        request3.setPeriodADays(10);
//        request3.setPeriodBDays(6);
//        request3.setOrderDeliveryDays(4);
//        request3.setLast7DaysAvgSales(5);
//        request3.setLast30DaysAvgSales(7);
//        request3.setIn7DaysAvgSales(6);
//        request3.setPeriodDaysVariance(0);
//        request3.setSampleNum(100);
//        request3.setMinOrderQ(40);
//        request3.setSalesPeriod(4);
//        request3.setSkuProperty("常规品");
//        request3.setPeriodAEstimateVariance(20);
//        request3.setPeriodBEstimateVariance(15);
//        request3.setSkuId("sku4");
//        request3.setSafetyStockModel("residual");
//        request3.setReplenishModel("短保");
//        requestList.add(request3);
//
//
//
//        ReplRequest request4 = new ReplRequest();
//        request4.setPeriodASales(200);
//        request4.setPeriodBSales(140);
//        request4.setDms(3);
//        request4.setSafetyStockDays(2);
//        request4.setMinDisplayRequire(10);
//        request4.setMinOrderQ(12);
//        request4.setStock(15);
//        request4.setTransferStock(0);
//        request4.setUnit(10);
//        request4.setPeriodADays(10);
//        request4.setPeriodBDays(6);
//        request4.setOrderDeliveryDays(4);
//        request4.setLast7DaysAvgSales(5);
//        request4.setLast30DaysAvgSales(8);
//        request4.setIn7DaysAvgSales(6);
//        request4.setPeriodDaysVariance(0);
//        request4.setSampleNum(100);
//        request4.setMinOrderQ(40);
//        request4.setSalesPeriod(4);
//        request4.setSkuProperty("常规品");
//        request4.setPeriodAEstimateVariance(20);
//        request4.setPeriodBEstimateVariance(15);
//        request4.setSkuId("sku4");
//        request4.setSafetyStockModel("residual");
//        request4.setReplenishModel("短保");
//        requestList.add(request4);


//        ReplRequest request5 = new ReplRequest();
//        request5.setPeriodASales(200);
//        request5.setPeriodBSales(140);
//        request5.setDms(3);
//        request5.setSafetyStockDays(2);
//        request5.setMinDisplayRequire(10);
//        request5.setMinOrderQ(12);
//        request5.setStock(15);
//        request5.setTransferStock(0);
//        request5.setUnit(10);
//        request5.setPeriodADays(10);
//        request5.setPeriodBDays(6);
//        request5.setOrderDeliveryDays(4);
//        request5.setLast7DaysAvgSales(5);
//        request5.setLast30DaysAvgSales(8);
//        request5.setIn7DaysAvgSales(6);
//        request5.setPeriodDaysVariance(0);
//        request5.setSampleNum(100);
//        request5.setMinOrderQ(40);
//        request5.setSalesPeriod(4);
//        request5.setSkuProperty("常规品");
//        request5.setPeriodAEstimateVariance(20);
//        request5.setPeriodBEstimateVariance(15);
//        request5.setSkuId("sku5");
//        request5.setSafetyStockModel("residual");
//        request5.setReplenishModel("日清");
//        requestList.add(request5);
//
//
//        ReplRequest request6 = new ReplRequest();
//        request6.setPeriodASales(200);
//        request6.setPeriodBSales(140);
//        request6.setDms(3);
//        request6.setSafetyStockDays(2);
//        request6.setMinDisplayRequire(10);
//        request6.setMinOrderQ(12);
//        request6.setStock(150);
//        request6.setTransferStock(0);
//        request6.setUnit(10);
//        request6.setPeriodADays(10);
//        request6.setPeriodBDays(6);
//        request6.setOrderDeliveryDays(4);
//        request6.setLast7DaysAvgSales(5);
//        request6.setLast30DaysAvgSales(8);
//        request6.setIn7DaysAvgSales(6);
//        request6.setPeriodDaysVariance(0);
//        request6.setSampleNum(100);
//        request6.setMinOrderQ(40);
//        request6.setSalesPeriod(4);
//        request6.setSkuProperty("常规品");
//        request6.setPeriodAEstimateVariance(20);
//        request6.setPeriodBEstimateVariance(15);
//        request6.setSkuId("sku5");
//        request6.setSafetyStockModel("residual");
//        request6.setReplenishModel("日清");
//        requestList.add(request6);
//
//        ReplRequest request7 = new ReplRequest();
//        request7.setPeriodASales(200);
//        request7.setPeriodBSales(140);
//        request7.setDms(3);
//        request7.setSafetyStockDays(2);
//        request7.setMinDisplayRequire(10);
//        request7.setMinOrderQ(12);
//        request7.setStock(150);
//        request7.setTransferStock(0);
//        request7.setUnit(10);
//        request7.setPeriodADays(10);
//        request7.setPeriodBDays(6);
//        request7.setOrderDeliveryDays(4);
//        request7.setLast7DaysAvgSales(5);
//        request7.setLast30DaysAvgSales(8);
//        request7.setIn7DaysAvgSales(6);
//        request7.setPeriodDaysVariance(0);
//        request7.setSampleNum(100);
//        request7.setMinOrderQ(40);
//        request7.setSalesPeriod(4);
//        request7.setSkuProperty("常规品");
//        request7.setPeriodAEstimateVariance(20);
//        request7.setPeriodBEstimateVariance(15);
//        request7.setSkuId("sku7");
//        request7.setSafetyStockModel("news");
//        request7.setWarrantyPeriods(182.0);
//        requestList.add(request7);
//
//

        ReplRequest request8 = new ReplRequest();
        request8.setPeriodASales(200.0);
        request8.setPeriodBSales(140.0);
        request8.setDms(3.0);
        request8.setSafetyStockDays(2.0);
        request8.setMinDisplayRequire(10.0);
        request8.setMinOrderQ(12.0);
        request8.setStock(150.0);
        request8.setTransferStock(0.0);
        request8.setUnit(10.0);
        request8.setPeriodADays(10.0);
        request8.setPeriodBDays(6.0);
        request8.setOrderDeliveryDays(4.0);
        request8.setLast7DaysAvgSales(5.0);
        request8.setLast30DaysAvgSales(8.0);
        request8.setIn7DaysAvgSales(6.0);
        request8.setPeriodDaysVariance(0.0);
        request8.setSampleNum(100.0);
        request8.setMinOrderQ(40.0);
        request8.setSalesPeriod(4.0);
        request8.setSkuProperty("常规品");
        request8.setPeriodAEstimateVariance(20.0);
        request8.setPeriodBEstimateVariance(15.0);
        request8.setSkuId("sku8");
        request8.setSafetyStockModel("news");
        request8.setWarrantyPeriods(95.0);
        request8.setShopId("1002");
        requestList.add(request8);

        ReplRequest request9 = new ReplRequest();
        request9.setShopId("1002");
        request9.setPeriodASales(200.0);
        request9.setPeriodBSales(140.0);
        request9.setDms(3.0);
        request9.setSafetyStockDays(2.0);
        request9.setMinDisplayRequire(220.0);
        request9.setMinOrderQ(12.0);
        request9.setStock(150.0);
        request9.setTransferStock(0.0);
        request9.setUnit(10.0);
        request9.setPeriodADays(10.0);
        request9.setPeriodBDays(6.0);
        request9.setOrderDeliveryDays(4.0);
        request9.setLast7DaysAvgSales(5.0);
        request9.setLast30DaysAvgSales(8.0);
        request9.setIn7DaysAvgSales(6.0);
        request9.setPeriodDaysVariance(0.0);
        request9.setSampleNum(100.0);
        request9.setMinOrderQ(300.0);
        request9.setSalesPeriod(8.0);
        request9.setSkuProperty("常规品");
        request9.setPeriodAEstimateVariance(20.0);
        request9.setPeriodBEstimateVariance(15.0);
        request9.setSkuId("sku9");
        request9.setSafetyStockModel("news");
        request9.setWarrantyPeriods(60.0);
        requestList.add(request9);


        ReplRequest request10 = new ReplRequest();
        request10.setShopId("1002");
        request10.setPeriodASales(200.0);
        request10.setPeriodBSales(140.0);
        request10.setDms(3.0);
        request10.setSafetyStockDays(2.0);
        request10.setMinDisplayRequire(220.0);
        request10.setMinOrderQ(12.0);
        request10.setStock(150.0);
        request10.setTransferStock(0.0);
        request10.setUnit(10.0);
        request10.setPeriodADays(10.0);
        request10.setPeriodBDays(6.0);
        request10.setOrderDeliveryDays(4.0);
        request10.setLast7DaysAvgSales(5.0);
        request10.setLast30DaysAvgSales(8.0);
        request10.setIn7DaysAvgSales(6.0);
        request10.setPeriodDaysVariance(0.0);
        request10.setSampleNum(100.0);
        request10.setMinOrderQ(40.0);
        request10.setSalesPeriod(1.0);
        request10.setSkuProperty("常规品");
        request10.setPeriodAEstimateVariance(20.0);
        request10.setPeriodBEstimateVariance(15.0);
        request10.setSkuId("sku10");
        request10.setSafetyStockModel("news");
        request10.setWarrantyPeriods(60.0);
        requestList.add(request10);


        ReplRequest request11 = new ReplRequest();
        request11.setPeriodASales(200.0);
        request11.setPeriodBSales(140.0);
        request11.setDms(3.0);
        request11.setSafetyStockDays(2.0);
        request11.setMinDisplayRequire(220.0);
        request11.setMinOrderQ(12.0);
        request11.setStock(300.0);
        request11.setTransferStock(20.0);
        request11.setUnit(10.0);
        request11.setPeriodADays(10.0);
        request11.setPeriodBDays(6.0);
        request11.setOrderDeliveryDays(4.0);
        request11.setLast7DaysAvgSales(5.0);
        request11.setLast30DaysAvgSales(8.0);
        request11.setIn7DaysAvgSales(6.0);
        request11.setPeriodDaysVariance(0.0);
        request11.setSampleNum(100.0);
        request11.setMinOrderQ(40.0);
        request11.setSalesPeriod(1.0);
        request11.setSkuProperty("常规品");
        request11.setPeriodAEstimateVariance(20.0);
        request11.setPeriodBEstimateVariance(15.0);
        request11.setSkuId("sku11");
        request11.setSafetyStockModel("news");
        request11.setWarrantyPeriods(60.0);
        requestList.add(request11);
        List<Double> history30DaysSales = new ArrayList<>();
        Random r = new Random(100);

        for (int i = 0; i < 30; i++) {
            double sales = r.nextDouble() + 20;
            history30DaysSales.add(sales);

        }
        System.out.println("hisotry30DaysSales" + history30DaysSales);
        ReplRequest request12 = new ReplRequest();
        request12.setPeriodASales(200.0);
        request12.setPeriodBSales(140.0);
        request12.setDms(3.0);
        request12.setSafetyStockDays(2.0);
        request12.setMinDisplayRequire(220.0);
        request12.setMinOrderQ(12.0);
        request12.setStock(300.0);
        request12.setTransferStock(20.0);
        request12.setUnit(10.0);
        request12.setPeriodADays(10.0);
        request12.setPeriodBDays(6.0);
        request12.setOrderDeliveryDays(4.0);
        request12.setLast7DaysAvgSales(5.0);
        request12.setLast30DaysAvgSales(8.0);
        request12.setIn7DaysAvgSales(6.0);
        request12.setPeriodDaysVariance(0.0);
        request12.setSampleNum(100.0);
        request12.setMinOrderQ(40.0);
        request12.setSalesPeriod(1.0);
        request12.setSkuProperty("常规品");
        request12.setPeriodAEstimateVariance(20.0);
        request12.setPeriodBEstimateVariance(15.0);
        request12.setSkuId("sku12");
        request12.setSafetyStockModel("boostrap");
        request12.setReplenishModel("常规");
        request12.setHistory30DaysSales(history30DaysSales);
        request12.setWarrantyPeriods(60.0);
        requestList.add(request12);


        ReplRequest request13 = new ReplRequest();
        request13.setPeriodASales(200.0);
        request13.setPeriodBSales(140.0);
        request13.setDms(3.0);
        request13.setSafetyStockDays(2.0);
        request13.setMinDisplayRequire(100.0);
        request13.setMinOrderQ(12.0);
        request13.setStock(100.0);
        request13.setTransferStock(20.0);
        request13.setUnit(10.0);
        request13.setPeriodADays(10.0);
        request13.setPeriodBDays(6.0);
        request13.setOrderDeliveryDays(4.0);
        request13.setLast7DaysAvgSales(5.0);
        request13.setLast30DaysAvgSales(8.0);
        request13.setIn7DaysAvgSales(6.0);
        request13.setPeriodDaysVariance(0.0);
        request13.setSampleNum(100.0);
        request13.setMinOrderQ(40.0);
        request13.setSalesPeriod(1.0);
        request13.setSkuProperty("常规品");
        request13.setPeriodAEstimateVariance(20.0);
        request13.setPeriodBEstimateVariance(15.0);
        request13.setSkuId("sku13");
        request13.setSafetyStockModel("boostrap");
        request13.setReplenishModel("常规");
        request13.setHistory30DaysSales(history30DaysSales);
        request13.setWarrantyPeriods(60.0);
        requestList.add(request13);


        ReplRequest request14 = new ReplRequest();
        request14.setPeriodASales(200.0);
        request14.setPeriodBSales(140.0);
        request14.setDms(3.0);
        request14.setSafetyStockDays(2.0);
        request14.setMinDisplayRequire(200.0);
        request14.setMinOrderQ(12.0);
        request14.setStock(50.0);
        request14.setTransferStock(20.0);
        request14.setUnit(10.0);
        request14.setPeriodADays(10.0);
        request14.setPeriodBDays(6.0);
        request14.setOrderDeliveryDays(4.0);
        request14.setLast7DaysAvgSales(5.0);
        request14.setLast30DaysAvgSales(8.0);
        request14.setIn7DaysAvgSales(6.0);
        request14.setPeriodDaysVariance(0.0);
        request14.setSampleNum(100.0);
        request14.setMinOrderQ(40.0);
        request14.setSalesPeriod(1.0);
        request14.setSkuProperty("常规品");
        request14.setPeriodAEstimateVariance(20.0);
        request14.setPeriodBEstimateVariance(15.0);
        request14.setSkuId("sku14");
        request14.setSafetyStockModel("boostrap");
        request14.setReplenishModel("常规");
        request14.setWarrantyPeriods(60.0);
        requestList.add(request14);


        ReplRequest request15 = new ReplRequest();
        request15.setPeriodASales(200.0);
        request15.setPeriodBSales(140.0);
        request15.setDms(3.0);
        request15.setSafetyStockDays(2.0);
        request15.setMinDisplayRequire(200.0);
        request15.setMinOrderQ(12.0);
        request15.setStock(50.0);
        request15.setTransferStock(20.0);
        request15.setUnit(10.0);
        request15.setPeriodADays(10.0);
        request15.setPeriodBDays(6.0);
        request15.setOrderDeliveryDays(4.0);
        request15.setLast7DaysAvgSales(5.0);
        request15.setLast30DaysAvgSales(8.0);
        request15.setIn7DaysAvgSales(6.0);
        request15.setPeriodDaysVariance(0.0);
        request15.setSampleNum(100.0);
        request15.setMinOrderQ(40.0);
        request15.setSalesPeriod(1.0);
        request15.setSkuProperty("常规品");
        request15.setPeriodAEstimateVariance(20.0);
        request15.setPeriodBEstimateVariance(15.0);
        request15.setSkuId("sku15");
        request15.setSafetyStockModel("boostrap");
        request15.setReplenishModel("常规");
        request15.setWarrantyPeriods(60.0);
        request15.setHistory30DaysSales(history30DaysSales.subList(0, 7));
        requestList.add(request15);



        ReplRequest request16 = new ReplRequest();
        request16.setPeriodASales(200.0);
        request16.setPeriodBSales(140.0);
        request16.setDms(3.0);
        request16.setSafetyStockDays(2.0);
        request16.setMinDisplayRequire(200.0);
        request16.setMinOrderQ(12.0);
        request16.setStock(50.0);
        request16.setTransferStock(20.0);
        request16.setUnit(10.0);
        request16.setPeriodADays(10.0);
        request16.setPeriodBDays(6.0);
        request16.setOrderDeliveryDays(4.0);
        request16.setLast7DaysAvgSales(5.0);
        request16.setLast30DaysAvgSales(8.0);
        request16.setIn7DaysAvgSales(6.0);
        request16.setPeriodDaysVariance(0.0);
        request16.setSampleNum(100.0);
        request16.setMinOrderQ(40.0);
        request16.setSalesPeriod(1.0);
        request16.setSkuProperty("常规品");
        request16.setPeriodAEstimateVariance(20.0);
        request16.setPeriodBEstimateVariance(15.0);
        request16.setSkuId("sku16");
        request16.setSafetyStockModel("boostrap");
        request16.setReplenishModel("常规");
        request16.setWarrantyPeriods(60.0);
        request16.setHistory30DaysSales(history30DaysSales.subList(0, 4));
        request16.setHistory30DaysSalesLength(4);
        requestList.add(request16);



        ReplRequest request17= new ReplRequest();
        request17.setPeriodASales(200.0);
        request17.setPeriodBSales(140.0);
        request17.setDms(22.0);
        request17.setSafetyStockDays(2.0);
        request17.setMinDisplayRequire(200.0);
        request17.setMinOrderQ(12.0);
        request17.setStock(50.0);
        request17.setTransferStock(20.0);
        request17.setUnit(10.0);
        request17.setPeriodADays(10.0);
        request17.setPeriodBDays(6.0);
        request17.setOrderDeliveryDays(4.0);
        request17.setLast7DaysAvgSales(5.0);
        request17.setLast30DaysAvgSales(8.0);
        request17.setIn7DaysAvgSales(6.0);
        request17.setPeriodDaysVariance(0.0);
        request17.setSampleNum(100.0);
        request17.setMinOrderQ(40.0);
        request17.setSalesPeriod(1.0);
        request17.setSkuProperty("常规品");
        request17.setPeriodAEstimateVariance(20.0);
        request17.setPeriodBEstimateVariance(15.0);
        request17.setSkuId("sku17");
        request17.setSafetyStockModel("boostrap");
        request17.setReplenishModel("常规");
        request17.setWarrantyPeriods(60.0);
        request17.setHistory30DaysSales(history30DaysSales.subList(0, 4));
        request17.setHistory30DaysSalesLength(4);
        requestList.add(request17);


        ReplRequest request18= new ReplRequest();
        request18.setPeriodASales(200.0);
        request18.setPeriodBSales(140.0);
        request18.setDms(22.0);
        request18.setSafetyStockDays(2.0);
        request18.setMinDisplayRequire(200.0);
        request18.setMinOrderQ(12.0);
        request18.setStock(50.0);
        request18.setTransferStock(20.0);
        request18.setUnit(10.0);
        request18.setPeriodADays(10.0);
        request18.setPeriodBDays(6.0);
        request18.setOrderDeliveryDays(4.0);
        request18.setLast7DaysAvgSales(5.0);
        request18.setLast30DaysAvgSales(8.0);
        request18.setIn7DaysAvgSales(6.0);
        request18.setPeriodDaysVariance(0.0);
        request18.setSampleNum(100.0);
        request18.setMinOrderQ(40.0);
        request18.setMinInStock(10.0);
        request18.setSalesPeriod(1.0);
        request18.setSkuProperty("常规品");
        request18.setPeriodAEstimateVariance(20.0);
        request18.setPeriodBEstimateVariance(15.0);
        request18.setSkuId("sku18");
        request18.setReplenishModel("R_Q");
        request18.setWarrantyPeriods(60.0);
        requestList.add(request18);


        ReplRequest request19= new ReplRequest();
        request19.setPeriodASales(200.0);
        request19.setPeriodBSales(140.0);
        request19.setDms(22.0);
        request19.setSafetyStockDays(2.0);
        request19.setMinDisplayRequire(200.0);
        request19.setMinOrderQ(12.0);
        request19.setStock(200.0);
        request19.setTransferStock(20.0);
        request19.setUnit(10.0);
        request19.setPeriodADays(10.0);
        request19.setPeriodBDays(6.0);
        request19.setOrderDeliveryDays(4.0);
        request19.setLast7DaysAvgSales(5.0);
        request19.setLast30DaysAvgSales(8.0);
        request19.setIn7DaysAvgSales(6.0);
        request19.setPeriodDaysVariance(0.0);
        request19.setSampleNum(100.0);
        request19.setMinOrderQ(40.0);
        request19.setMinInStock(10.0);
        request19.setSalesPeriod(1.0);
        request19.setSkuProperty("常规品");
        request19.setPeriodAEstimateVariance(20.0);
        request19.setPeriodBEstimateVariance(15.0);
        request19.setSkuId("sku19");
        request19.setReplenishModel("R_Q");
        request19.setWarrantyPeriods(60.0);
        requestList.add(request19);


        ReplRequest request20= new ReplRequest();
        request20.setPeriodASales(200.0);
        request20.setPeriodBSales(140.0);
        request20.setDms(22.0);
        request20.setSafetyStockDays(2.0);
        request20.setMinDisplayRequire(200.0);
        request20.setMinOrderQ(12.0);
        request20.setStock(200.0);
        request20.setTransferStock(20.0);
        request20.setUnit(10.0);
        request20.setPeriodADays(10.0);
        request20.setPeriodBDays(6.0);
        request20.setOrderDeliveryDays(4.0);
        request20.setLast7DaysAvgSales(5.0);
        request20.setLast30DaysAvgSales(8.0);
        request20.setIn7DaysAvgSales(6.0);
        request20.setPeriodDaysVariance(0.0);
        request20.setSampleNum(100.0);
        request20.setMinOrderQ(40.0);
        request20.setMinInStock(10.0);
        request20.setSalesPeriod(1.0);
        request20.setSkuProperty("常规品");
        request20.setPeriodAEstimateVariance(20.0);
        request20.setPeriodBEstimateVariance(15.0);
        request20.setSkuId("sku20");
        request20.setSafetyStockModel("outliers");
        request20.setWarrantyPeriods(60.0);
        requestList.add(request20);



        ReplRequest request21= new ReplRequest();
        request21.setPeriodASales(200.0);
        request21.setPeriodBSales(140.0);
        request21.setDms(22.0);
        request21.setSafetyStockDays(2.0);
        request21.setMinDisplayRequire(200.0);
        request21.setMinOrderQ(12.0);
        request21.setStock(200.0);
        request21.setTransferStock(20.0);
        request21.setUnit(10.0);
        request21.setPeriodADays(10.0);
        request21.setPeriodBDays(6.0);
        request21.setOrderDeliveryDays(4.0);
        request21.setLast7DaysAvgSales(5.0);
        request21.setLast30DaysAvgSales(8.0);
        request21.setIn7DaysAvgSales(6.0);
        request21.setPeriodDaysVariance(0.0);
        request21.setSampleNum(100.0);
        request21.setMinOrderQ(40.0);
        request21.setMinInStock(10.0);
        request21.setSalesPeriod(1.0);
        request21.setSkuProperty("tail");
        request21.setPeriodAEstimateVariance(20.0);
        request21.setPeriodBEstimateVariance(15.0);
        request21.setSkuId("sku20");
        request21.setWarrantyPeriods(60.0);
        requestList.add(request21);



        ReplRequest request22= new ReplRequest();
        request22.setPeriodASales(200.0);
        request22.setPeriodBSales(140.0);
        request22.setDms(22.0);
        request22.setSafetyStockDays(2.0);
        request22.setMinDisplayRequire(200.0);
        request22.setMinOrderQ(12.0);
        request22.setStock(200.0);
        request22.setTransferStock(100.0);
        request22.setUnit(10.0);
        request22.setPeriodADays(10.0);
        request22.setPeriodBDays(6.0);
        request22.setOrderDeliveryDays(4.0);
        request22.setLast7DaysAvgSales(5.0);
        request22.setLast30DaysAvgSales(8.0);
        request22.setIn7DaysAvgSales(6.0);
        request22.setPeriodDaysVariance(0.0);
        request22.setSampleNum(100.0);
        request22.setMinOrderQ(40.0);
        request22.setMinInStock(10.0);
        request22.setSalesPeriod(1.0);
        request22.setSkuProperty("tail");
        request22.setSkuId("sku20");
        request22.setWarrantyPeriods(60.0);
        requestList.add(request22);



        ReplRequest request23= new ReplRequest();
        request23.setPeriodASales(200.0);
        request23.setPeriodBSales(140.0);
        request23.setDms(22.0);
        request23.setSafetyStockDays(2.0);
        request23.setMinDisplayRequire(200.0);
        request23.setMinOrderQ(12.0);
        request23.setStock(50.0);
        request23.setTransferStock(20.0);
        request23.setUnit(10.0);
        request23.setPeriodADays(10.0);
        request23.setPeriodBDays(6.0);
        request23.setOrderDeliveryDays(4.0);
        request23.setLast7DaysAvgSales(5.0);
        request23.setLast30DaysAvgSales(8.0);
        request23.setIn7DaysAvgSales(6.0);
        request23.setPeriodDaysVariance(0.0);
        request23.setSampleNum(100.0);
        request23.setMinOrderQ(40.0);
        request23.setMinInStock(10.0);
        request23.setSalesPeriod(1.0);
        request23.setSkuProperty("ls");
        request23.setSkuId("sku20");
        request23.setWarrantyPeriods(60.0);
        requestList.add(request23);



        ReplRequest request24= new ReplRequest();
        request24.setPeriodASales(200.0);
        request24.setPeriodBSales(140.0);
        request24.setDms(22.0);
        request24.setSafetyStockDays(2.0);
        request24.setMinDisplayRequire(200.0);
        request24.setMinOrderQ(12.0);
        request24.setStock(50.0);
        request24.setTransferStock(20.0);
        request24.setUnit(10.0);
        request24.setPeriodADays(10.0);
        request24.setPeriodBDays(6.0);
        request24.setOrderDeliveryDays(4.0);
        request24.setLast7DaysAvgSales(5.0);
        request24.setLast30DaysAvgSales(8.0);
        request24.setIn7DaysAvgSales(6.0);
        request24.setPeriodDaysVariance(0.0);
        request24.setSampleNum(100.0);
        request24.setMinOrderQ(40.0);
        request24.setMinInStock(10.0);
        request24.setSalesPeriod(1.0);
        request24.setSkuProperty("normal");
        request24.setSkuId("sku20");
        request24.setWarrantyPeriods(60.0);
        request24.setPeriodAEstimateVariance(20.0);
        requestList.add(request24);


        ReplRequest request25= new ReplRequest();
        request25.setPeriodASales(200.0);
        request25.setPeriodBSales(140.0);
        request25.setDms(22.0);
        request25.setSafetyStockDays(2.0);
        request25.setMinDisplayRequire(200.0);
        request25.setMinOrderQ(12.0);
        request25.setStock(50.0);
        request25.setTransferStock(20.0);
        request25.setUnit(10.0);
        request25.setPeriodADays(10.0);
        request25.setPeriodBDays(6.0);
        request25.setOrderDeliveryDays(4.0);
        request25.setLast7DaysAvgSales(5.0);
        request25.setLast30DaysAvgSales(8.0);
        request25.setIn7DaysAvgSales(6.0);
        request25.setPeriodDaysVariance(0.0);
        request25.setSampleNum(100.0);
        request25.setMinOrderQ(40.0);
        request25.setMinInStock(10.0);
        request25.setSalesPeriod(1.0);
        request25.setSkuProperty("normal");
        request25.setSkuId("sku20");
        request25.setWarrantyPeriods(60.0);
        request25.setPeriodAEstimateVariance(20.0);
        request25.setIsNew(true);
        requestList.add(request25);


        ReplRequest request26= new ReplRequest();
        request26.setPeriodASales(200.0);
        request26.setPeriodBSales(140.0);
        request26.setDms(22.0);
        request26.setSafetyStockDays(2.0);
        request26.setMinDisplayRequire(200.0);
        request26.setMinOrderQ(12.0);
        request26.setStock(200.0);
        request26.setTransferStock(100.0);
        request26.setUnit(10.0);
        request26.setPeriodADays(10.0);
        request26.setPeriodBDays(6.0);
        request26.setOrderDeliveryDays(4.0);
        request26.setLast7DaysAvgSales(5.0);
        request26.setLast30DaysAvgSales(8.0);
        request26.setIn7DaysAvgSales(6.0);
        request26.setPeriodDaysVariance(0.0);
        request26.setSampleNum(100.0);
        request26.setMinOrderQ(40.0);
        request26.setMinInStock(10.0);
        request26.setSalesPeriod(1.0);
        request26.setSkuProperty("normal");
        request26.setSkuId("sku20");
        request26.setWarrantyPeriods(60.0);
        request26.setPeriodAEstimateVariance(20.0);
        request26.setIsNew(true);
        requestList.add(request26);


        ModelParam modelParam = ParseReplConfig.getConfigModelParam();
        String date = "2022-05-17";
        List<ReplResult> results = replenishmentService.processRequest(requestList, modelParam, date, null, 1,true);


        log.info("done");

    }
}
