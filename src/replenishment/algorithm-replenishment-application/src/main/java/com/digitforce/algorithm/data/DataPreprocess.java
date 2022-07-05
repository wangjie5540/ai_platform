package com.digitforce.algorithm.data;

import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.dto.data.PreprocessData;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Builder
@Slf4j
public class DataPreprocess {


    // 大仓内商品补货参数列表
    private List<ReplRequest> requests;

    // 大仓库存数据
    private Map<String, Object> warehouseStock;

    // 门店与大仓补货对应关系
    private Map<String, List<String>> replGoodsList;

    // 大仓id
    private String warehouseId;

    // 预测销量 key为goodsId + "," +shopId + "," + daysInt
    private Map<String, Double> predictedSales;

    List<String> shopIds;

    private String date;



    public List<ReplRequest> generateReplList() {
        List<ReplRequest> nodeList = new ArrayList<>();

        requests = requests.stream().filter(e->shopIds.contains(e.getShopId())).collect(Collectors.toList());

        int level = 2;

        for (Map.Entry<String, List<String>> entry:replGoodsList.entrySet()) {
            String shopId = entry.getKey().split(",")[0];
            String goodsId = entry.getKey().split(",")[1];

            List<String> subShopList = entry.getValue().stream().map(e->e.substring(0, 4)).collect(Collectors.toList());

            // 现在只有门店级别的requests
            List<ReplRequest> subRequests = requests.stream().filter(e->e.getSkuId().equals(goodsId) && subShopList.contains(e.getShopId())).collect(Collectors.toList());
            // 如果sku不满足补货的要求（即不在计算补货的列表中，同样也不计算多级补货，不生成对应的request对象）
            if (subRequests != null && !subRequests.isEmpty()) {

                ReplRequest node = generateParentRequest(subRequests, subRequests, warehouseStock, goodsId, shopId, level);
                nodeList.add(node);
            }
        }
        return  nodeList;
    }

    private ReplRequest generateParentRequest(List<ReplRequest> subRequests, List<ReplRequest> subNodeRequests, Map<String, Object> dataMap, String goodsId, String shopId, int level) {
        ReplRequest node = initialParentRequest(subRequests.get(0), goodsId, shopId, level);
        node = generateMockData(node, subRequests, true);
        // 门店数据加和
        node = processShopProperty(node, subRequests);
        // 读取数据
        node = addValueFromData(node, dataMap);
        return node;
    }


    /** 当前节点（非叶子节点）初始化
     *
     * @param childRequest
     * @param goodsId
     * @param shopId
     * @param level
     * @return
     */
    private ReplRequest initialParentRequest(ReplRequest childRequest, String goodsId, String shopId, int level) {
        ReplRequest node = new ReplRequest();
        node.setDefaultValue();
        node = PreprocessData.processReplRequest(childRequest);
        node.setSkuId(goodsId);
        node.setShopId(shopId);
        node.setLevel(level);
        return node;
    }





    /** 加入node上的mockdata如销量，残差等数据
     *
     * @param parentReplRequest
     * @param requests 门店级别的requests，用来mock此sku所在门店j在当前level的预测销量,残差。当前节点的销量预测与残差通过requests计算得来，并可通过requests生成branchRequests
     * @return
     */
    public ReplRequest generateMockData(ReplRequest parentReplRequest, List<ReplRequest> requests, boolean rootFlag) {
        Random r = new Random(100);

        double minPeriodADays = requests.stream().mapToDouble(e->e.getPeriodADays()).max().getAsDouble();
//        double minPeriodBDays = requests.stream().mapToDouble(e->e.getPeriodBDays()).max().getAsDouble();
        int warehousePeriodALimit = (int) Math.ceil(14 - minPeriodADays);
        int warehousePeriodA = Math.max(r.nextInt(warehousePeriodALimit),2) ;

        // resetPeriodADays 送货天数
        double leadTime = r.nextInt(warehousePeriodA) ;

        // resetPeriodBDays 订货间隔天数
        double replPeriod = warehousePeriodA - leadTime;
        parentReplRequest.setPeriodBDays(replPeriod);


        double periodADays = replPeriod + leadTime;
        parentReplRequest.setPeriodADays(replPeriod + leadTime);
        parentReplRequest.setOrderDeliveryDays(leadTime);

        double parentPeriodASales = 0.0;
        double parentPeriodBSales = 0.0;

        Map<String, Double> goodsPredictedSales = predictedSales.entrySet().stream()
                .filter(e->e.getKey().contains(parentReplRequest.getSkuId()))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));

        Map<String, ReplRequest> branchRequest = new HashMap<>();
        int i = 1;
        for (ReplRequest request:requests) {
            ReplRequest newRequest = request.clone();
            String shopId = request.getShopId();

            newRequest = PreprocessData.processReplRequest(newRequest);
            newRequest.setLevel(2);

            double requestLevelPeriodBDays = replPeriod + newRequest.getPeriodBDays();
            double requestLevelPeriodADays = periodADays + newRequest.getPeriodADays();

            newRequest.setPeriodBDays(requestLevelPeriodBDays);
            newRequest.setPeriodADays(requestLevelPeriodADays);


            double newPeriodASales = goodsPredictedSales.entrySet().stream()
                    .filter(e-> e.getKey().split(",")[1].equals(shopId) && Integer.valueOf(e.getKey().split(",")[2]) <= requestLevelPeriodADays)
                    .mapToDouble(ee->ee.getValue()).sum();
            newRequest.setPeriodASales(newPeriodASales);
            parentPeriodASales += newPeriodASales;


            // resetPeriodBSales
            double newPeriodBSales = goodsPredictedSales.entrySet().stream()
                    .filter(e-> e.getKey().split(",")[1].equals(shopId) && Integer.valueOf(e.getKey().split(",")[2]) <= requestLevelPeriodBDays)
                    .mapToDouble(ee->ee.getValue()).sum();
            newRequest.setPeriodBSales(newPeriodBSales);
            parentPeriodBSales += newPeriodBSales;


            double AVarianceTimes = r.nextDouble() * 2;
            double BVarianceTimes = r.nextDouble() * 2;
            // resetPeriodAEstimateVariance
            double periodAEstimateVariance = request.getPeriodAEstimateVariance() * AVarianceTimes;
            newRequest.setPeriodAEstimateVariance(periodAEstimateVariance);

            // resetPeriodBEstimateVariance
            double periodBEstiamteVariance = request.getPeriodBEstimateVariance() * BVarianceTimes;
            newRequest.setPeriodBEstimateVariance(periodBEstiamteVariance);

            // TODO:如果一个链路里有多个节点，需要按链路更新库存
            branchRequest.put(String.valueOf(i), newRequest);
            i+=1;
        }
        parentReplRequest.setPeriodASales(parentPeriodASales);
        parentReplRequest.setPeriodBSales(parentPeriodBSales);
        double parentPeriodAVariance = calculateParentCovariance(requests, "A");
        double parentPeriodBVariance = calculateParentCovariance(requests, "B");
        if (rootFlag) {
            parentPeriodAVariance += Math.pow(parentPeriodASales, 2) * parentReplRequest.getPeriodDaysVariance();
            parentPeriodBVariance += Math.pow(parentPeriodASales, 2) * parentReplRequest.getPeriodDaysVariance();
        }
        parentReplRequest.setPeriodAEstimateVariance(parentPeriodAVariance);
        parentReplRequest.setPeriodBEstimateVariance(parentPeriodBVariance);
        parentReplRequest.setBranchRequests(branchRequest);
        return parentReplRequest;
    }

    /**
     *
     * @param parentRequest
     * @param requests 所有门店的request列表
     * @return
     */
    private ReplRequest processShopProperty(ReplRequest parentRequest, List<ReplRequest> requests) {
        if (requests != null && !requests.isEmpty()) {
            // dms
            double dms = requests.stream().mapToDouble(e->e.getDms()).sum();

            // 历史30天均销
            double history30avg = requests.stream().mapToDouble(e -> e.getLast30DaysAvgSales()).sum();

            // 历史7天均销
            double history7avg = requests.stream().mapToDouble(e -> e.getLast7DaysAvgSales()).sum();

            // 未来7天预测均销
            double in7avg = requests.stream().mapToDouble(e -> e.getIn7DaysAvgSales()).sum();

            // 历史30天销量列表
            List<Double> nodeHistorySales = new ArrayList<>();
            for (int i = 0; i < requests.get(0).getHistory30DaysSalesLength(); i++) {
                int finalI = i;
                double dayiTotalSales = requests.stream().map(e -> e.getHistory30DaysSales().get(finalI)).mapToDouble(ee -> ee).sum();
                nodeHistorySales.add(dayiTotalSales);
            }

            //子节点所有将有库存（库内+在途）
            double nodeStock = requests.stream().mapToDouble(ee->ee.getStock() + ee.getTransferStock()).sum();

            //安全排面量
            double nodeMinDisplayRequire = requests.stream().mapToDouble(e->e.getMinDisplayRequire()).sum();

            parentRequest.setDms(dms);
            parentRequest.setLast30DaysAvgSales(history30avg);
            parentRequest.setLast7DaysAvgSales(history7avg);
            parentRequest.setIn7DaysAvgSales(in7avg);
            parentRequest.setHistory30DaysSales(nodeHistorySales);
            parentRequest.setHistory30DaysSalesLength(nodeHistorySales.size());
            parentRequest.setNodeStock(nodeStock);
            parentRequest.setMinDisplayRequire(parentRequest.getMinDisplayRequire() + nodeMinDisplayRequire);
        }
        return parentRequest;
    }


    /** 将读取数据的值添加到属性
     *
     * @param node
     * @param dataMap
     * @return
     */
    private ReplRequest addValueFromData(ReplRequest node, Map<String, Object> dataMap) {
        String goodsId = node.getSkuId();
        String warehouseId = node.getShopId();
        String key = warehouseId + "," + goodsId;
        double stock = (double) dataMap.getOrDefault(key, 0.0);
        node.setStock(stock);
        return node;
    }
    /** 相关性系数分子
     *
     * @param goods1Sales 历史30天销量数据，没有数据的补0
     * @param goods2Sales 历史30天销量数据，没有数据的补0
     * @param goods1Mean
     * @param goods2Mean
     * @return
     */
    private double generateNumerator(List<Double> goods1Sales, List<Double> goods2Sales, double goods1Mean, double goods2Mean) {
        double numerator = 0.0;
        // 补齐0
        if (goods1Sales.size() != goods2Sales.size()) {
            int diffLength = Math.max(goods1Sales.size(), goods2Sales.size()) - Math.min(goods1Sales.size(), goods2Sales.size());
            List<Double> addZeros = new ArrayList<>();
            for (int i = 0; i < diffLength; i++) addZeros.add(0.0);
            if (goods1Sales.size() > goods2Sales.size())  goods2Sales.addAll(addZeros);
            if (goods2Sales.size() > goods1Sales.size())  goods1Sales.addAll(addZeros);
        }

        int minLength = Math.min(goods1Sales.size(), goods2Sales.size());
        for (int i = 0; i < minLength; i++) {
            numerator += (goods1Sales.get(i) - goods1Mean) * (goods2Sales.get(i) - goods2Mean);
        }
        return numerator;
    }


    /** 相关性系数分母
     *
     * @param goods1Sales
     * @param goods2Sales
     * @param goods1Mean
     * @param goods2Mean
     * @return
     */
    private double generateDenominator(List<Double> goods1Sales, List<Double> goods2Sales, double goods1Mean, double goods2Mean) {
        double goods1Sum = 0.0;
        for (int i = 0; i < goods1Sales.size(); i++) {
            goods1Sum +=  Math.pow(goods1Sales.get(i) - goods1Mean, 2);
        }
        double goods2Sum = 0.0;
        for (int j = 0; j < goods2Sales.size(); j++) {
            goods2Sum += Math.pow(goods2Sales.get(j) - goods2Mean, 2);
        }
        return Math.sqrt(goods1Sum) * Math.sqrt(goods2Sum);
    }

    private double generateCorrelation(List<Double> goods1Sales, List<Double> goods2Sales) {
        double goods1Mean = 0.0;
        double goods2Mean = 0.0;
        if (goods1Sales != null && !goods1Sales.isEmpty())
            goods1Mean = goods1Sales.stream().mapToDouble(e->e).average().getAsDouble();
        if (goods2Sales != null && !goods2Sales.isEmpty())
            goods2Mean = goods2Sales.stream().mapToDouble(ee->ee).average().getAsDouble();
        double numerator = generateNumerator(goods1Sales, goods2Sales, goods1Mean, goods2Mean);
        if (numerator == 0.0) {
            return 0.0;
        } else {
            double denominator = generateDenominator(goods1Sales, goods2Sales, goods1Mean, goods2Mean);
            double correlation = numerator / denominator;
            // 如果correlation < 0.3则认为没有相关性
            if (Math.abs(correlation) < 0.3) {
                correlation = 0.0;
            }
            return correlation;
        }
    }


    /** 计算相关性
     *
     * @param requests
     * @param period
     * @return
     */
    private double calculateParentCovariance(List<ReplRequest> requests, String period) {
        double covarianceSum = 0.0;
        double varianceSum = 0.0;
        for (int i = 0; i < requests.size(); i++) {
            double goods1EstimateVariance;
            ReplRequest goods1 = requests.get(i);
            if ("A".equals(period)) {
                goods1EstimateVariance = goods1.getPeriodAEstimateVariance();
            } else {
                goods1EstimateVariance = goods1.getPeriodBEstimateVariance();
            }
            varianceSum += Math.pow(goods1EstimateVariance,2);
            for (int j = i+1; j < requests.size(); j++) {
                ReplRequest goods2 = requests.get(j);
                List<Double> goods1Sales = goods1.getHistory30DaysSales();
                List<Double> goods2Sales = goods2.getHistory30DaysSales();
                double correlation = generateCorrelation(goods1Sales, goods2Sales);
                if ("A".equals(period)) {
                    covarianceSum += 2 * correlation * goods1EstimateVariance * goods2.getPeriodAEstimateVariance();
                } else {
                    covarianceSum += 2 * correlation * goods1EstimateVariance * goods2.getPeriodBEstimateVariance();
                }
            }
        }
        return varianceSum + covarianceSum;

    }



}
