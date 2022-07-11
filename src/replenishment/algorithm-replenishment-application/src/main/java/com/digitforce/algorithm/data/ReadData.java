package com.digitforce.algorithm.data;
import com.digitforce.algorithm.dto.*;
import com.digitforce.algorithm.dto.data.DateProcessor;
import com.fasterxml.jackson.datatype.jsr310.ser.MonthDaySerializer;
import org.apache.ibatis.javassist.Loader;
import org.hibernate.validator.internal.engine.PredefinedScopeConfigurationImpl;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class ReadData {

    public List<ReplRequest> readReplRequest(String paramPath, String date, List<String> shopIds) {
        List<ReplRequest> requests = readParam(paramPath, date, shopIds);
        return requests;
    }


    /** 读取补货参数
     *
     * @param filePath
     * @param dt
     * @param shopIds
     * @return
     */
    public static List<ReplRequest> readParam(String filePath, String dt, List<String> shopIds) {
        List<ReplRequest> requests = new ArrayList<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        String daysBefore30 = DateProcessor.addDate(dt, -30);
        List<String> history30Days = DateProcessor.getDateList(daysBefore30, dt);


        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String convertDate = sdf.format(new SimpleDateFormat("yyyy-MM-dd").parse(dt));

            br = new BufferedReader(new FileReader(filePath));
            br.readLine();
            while ((line = br.readLine()) != null) {


                // use comma as separator
                String[] str = line.split(cvsSplitBy);

                if (str[31].equals("zbcc"))
                    continue;
                if (!convertDate.equals(str[33]) || ! shopIds.contains(str[1]))
//                if (!convertDate.equals(str[33]) || ! shopId.equals(str[1]))
                    continue;
                String shopId = str[1];

//                34 = "y_dt_list"
                double in7AvgSales = 0.0;
                if (str.length == 38) {
                   in7AvgSales = Double.valueOf(str[37]);
                }
                double last30AvgSales = Double.valueOf(str[36]);
                double last7AvgSales = Double.valueOf(str[35]);
                String date = String.valueOf(str[33]);
//                if (date.equals(dt))
//                    continue;
                String isNew = str[31];
                boolean isNewFlag;
                if ("1".equals(isNew)) {
                    isNewFlag = true;
                } else {
                    isNewFlag = false;
                }
                double r = Double.valueOf(str[30]);
                double warrantyPeriod = Double.valueOf(str[29]);
//                double sales_days = Double.valueOf(str[28]);
                String safetyStockModel = str[26];
                String replenishModel = str[25];
                double stock = Double.valueOf(str[20]);
                double transferStock = Double.valueOf(str[19]);
                double unit = Double.valueOf(str[17]);
                double minDisplayRequire = Double.valueOf(str[16]);
                double minOrderQ = Double.valueOf(str[15]);
                double salesPeriod = Double.valueOf(str[14]);
                double dms = Double.valueOf(str[13]);
                String bsLabelStr =str[12];
                boolean bsLabel;
                if ("1".equals(bsLabelStr)) {
                    bsLabel = true;
                } else {
                    bsLabel = false;
                }
                double predsD = Double.valueOf(str[11]);
                double varD = Double.valueOf(str[10]);
                double l = Double.valueOf(str[9]);
                double d = Double.valueOf(str[8]);
                double k = Double.valueOf(str[7]);
                double predsW = Double.valueOf(str[6]);
                double varW = Double.valueOf(str[5]);
                double w = Double.valueOf(str[2]);
                String goodsId = str[0];

                ReplRequest request = new ReplRequest();
                request.setLast7DaysAvgSales(last7AvgSales);
                request.setLast30DaysAvgSales(last30AvgSales);
                request.setIn7DaysAvgSales(in7AvgSales);
                request.setIsNew(isNewFlag);
                request.setMinInStock(r);
                request.setWarrantyPeriods(warrantyPeriod);
                request.setSalesPeriod(salesPeriod);
                request.setSafetyStockModel(safetyStockModel);
                request.setReplenishModel(replenishModel);
                request.setStock(stock);
                request.setTransferStock(transferStock);
                request.setUnit(unit);
                request.setMinDisplayRequire(minDisplayRequire);
                request.setMinOrderQ(minOrderQ);
                request.setDms(dms);
                request.setBestSellLabel(bsLabel);
                request.setPeriodBSales(predsD);
                request.setPeriodBEstimateVariance(varD);
                request.setOrderDeliveryDays(l);
                request.setPeriodBDays(d);
                request.setSampleNum(k);
                request.setPeriodASales(predsW);
                request.setPeriodAEstimateVariance(varW);
                request.setPeriodADays(w);
                request.setSkuId(goodsId);
                request.setShopId(shopId);
                request.setLevel(1);
                request.setDate(date);

//                String[] ylistStr = str[34].replaceAll("\\[|]|'", "").split("\\|");
//                List<Double> yList = Arrays.stream(ylistStr).map(e->Double.valueOf(e)).collect(Collectors.toList());

                List<Double> yList = new ArrayList<>();
                if (!str[34].equals("") && str[34] != null && !str[34].isEmpty() && !str[34].equals("nan")) {
                    List<String> history30SalesStr = Arrays.stream(str[34].replaceAll("\\[|]|'", "").split(" "))
                            .filter(value->!value.equals("")).collect(Collectors.toList());

                    for (String dateStr:history30Days) {
                        Optional<String> selectedDateStr = history30SalesStr.stream().filter(e->e.split("\\|")[1].equals(dateStr)).findAny();
                        if (selectedDateStr.isPresent()) {
                            String selectedSales = selectedDateStr.get().split("\\|")[0];
                            yList.add(Double.valueOf(selectedSales));
                        } else {
                            yList.add(0.0);
                        }
                    }


//                    yList = Arrays.stream(str[34].replaceAll("\\[|]|'", "").split(" "))
//                            .filter(value->!value.equals(""))
//                            .map(e -> e.split("\\|")[0]).map(ee -> Double.valueOf(ee)).collect(Collectors.toList());
//                    yList = yList.stream().filter(e->Double.compare(e, 0.0) != 0).collect(Collectors.toList());
                } else {
                    for (String dateStr:history30Days) {yList.add(0.0);}
                }
                request.setHistory30DaysSales(yList);
                request.setHistory30DaysSalesLength(yList.size());
                requests.add(request);

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return requests;
    }



    /** 读取多天补货参数
     *
     * @param filePath
     * @return
     */
    public static List<ReplRequest> readMultipleDaysParam(String filePath) {
        List<ReplRequest> requests = new ArrayList<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";




        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");


            br = new BufferedReader(new FileReader(filePath));
            br.readLine();
            while ((line = br.readLine()) != null) {


                // use comma as separator
                String[] str = line.split(cvsSplitBy);

                if (str[31].equals("zbcc"))
                    continue;

                String shopId = str[1];

//                34 = "y_dt_list"
                double in7AvgSales = 0.0;
                if (str.length == 38) {
                    in7AvgSales = Double.valueOf(str[37]);
                }
                double last30AvgSales = Double.valueOf(str[36]);
                double last7AvgSales = Double.valueOf(str[35]);
                String date = String.valueOf(str[33]);

                String convertDate = sdf.format(new SimpleDateFormat("yyyyMMdd").parse(date));
                String daysBefore30 = DateProcessor.addDate(convertDate, -30);
                List<String> history30Days = DateProcessor.getDateList(daysBefore30, convertDate);

                String isNew = str[31];
                boolean isNewFlag;
                if ("1".equals(isNew)) {
                    isNewFlag = true;
                } else {
                    isNewFlag = false;
                }
                double r = Double.valueOf(str[30]);
                double warrantyPeriod = Double.valueOf(str[29]);
//                double sales_days = Double.valueOf(str[28]);
                String safetyStockModel = str[26];
                String replenishModel = str[25];
                double stock = Double.valueOf(str[20]);
                double transferStock = Double.valueOf(str[19]);
                double unit = Double.valueOf(str[17]);
                double minDisplayRequire = Double.valueOf(str[16]);
                double minOrderQ = Double.valueOf(str[15]);
                double salesPeriod = Double.valueOf(str[14]);
                double dms = Double.valueOf(str[13]);
                String bsLabelStr =str[12];
                boolean bsLabel;
                if ("1".equals(bsLabelStr)) {
                    bsLabel = true;
                } else {
                    bsLabel = false;
                }
                double predsD = Double.valueOf(str[11]);
                double varD = Double.valueOf(str[10]);
                double l = Double.valueOf(str[9]);
                double d = Double.valueOf(str[8]);
                double k = Double.valueOf(str[7]);
                double predsW = Double.valueOf(str[6]);
                double varW = Double.valueOf(str[5]);
                double w = Double.valueOf(str[2]);
                String goodsId = str[0];

                ReplRequest request = new ReplRequest();
                request.setLast7DaysAvgSales(last7AvgSales);
                request.setLast30DaysAvgSales(last30AvgSales);
                request.setIn7DaysAvgSales(in7AvgSales);
                request.setIsNew(isNewFlag);
                request.setMinInStock(r);
                request.setWarrantyPeriods(warrantyPeriod);
                request.setSalesPeriod(salesPeriod);
                request.setSafetyStockModel(safetyStockModel);
                request.setReplenishModel(replenishModel);
                request.setStock(stock);
                request.setTransferStock(transferStock);
                request.setUnit(unit);
                request.setMinDisplayRequire(minDisplayRequire);
                request.setMinOrderQ(minOrderQ);
                request.setDms(dms);
                request.setBestSellLabel(bsLabel);
                request.setPeriodBSales(predsD);
                request.setPeriodBEstimateVariance(varD);
                request.setOrderDeliveryDays(l);
                request.setPeriodBDays(d);
                request.setSampleNum(k);
                request.setPeriodASales(predsW);
                request.setPeriodAEstimateVariance(varW);
                request.setPeriodADays(w);
                request.setSkuId(goodsId);
                request.setShopId(shopId);
                request.setLevel(1);
                request.setDate(date);

//                String[] ylistStr = str[34].replaceAll("\\[|]|'", "").split("\\|");
//                List<Double> yList = Arrays.stream(ylistStr).map(e->Double.valueOf(e)).collect(Collectors.toList());

                List<Double> yList = new ArrayList<>();
                if (!str[34].equals("") && str[34] != null && !str[34].isEmpty() && !str[34].equals("nan")) {
                    List<String> history30SalesStr = Arrays.stream(str[34].replaceAll("\\[|]|'", "").split(" "))
                            .filter(value->!value.equals("")).collect(Collectors.toList());

//                    System.out.println(goodsId + ", " + shopId + ", " + date);
                    for (String dateStr:history30Days) {
                        Optional<String> selectedDateStr = history30SalesStr.stream().filter(e->e.split("\\|")[1].equals(dateStr)).findAny();
                        if (selectedDateStr.isPresent()) {
                            String selectedSales = selectedDateStr.get().split("\\|")[0];
                            yList.add(Double.valueOf(selectedSales));
                        } else {
                            yList.add(0.0);
                        }
                    }


//                    yList = Arrays.stream(str[34].replaceAll("\\[|]|'", "").split(" "))
//                            .filter(value->!value.equals(""))
//                            .map(e -> e.split("\\|")[0]).map(ee -> Double.valueOf(ee)).collect(Collectors.toList());
//                    yList = yList.stream().filter(e->Double.compare(e, 0.0) != 0).collect(Collectors.toList());
                } else {
                    for (String dateStr:history30Days) {yList.add(0.0);}
                }
                request.setHistory30DaysSales(yList);
                request.setHistory30DaysSalesLength(yList.size());
                requests.add(request);

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return requests;
    }



    /** 获取指定大仓库存
     *
     * @param filePath
     * @param dt
     * @param warehouseId
     * @return
     */
    public static Map<String, Object> readWarehouseStock(String filePath, String dt, String warehouseId) {
        List<ReplRequest> requests = new ArrayList<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        Map<String, Object> stockMap = new HashMap<>();

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
//            br = new BufferedReader(new FileReader(filePath), "UTF-8");

            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] str = line.split(cvsSplitBy);
                if (!dt.equals(str[0]) || ! warehouseId.equals(str[1]))
                    continue;
                String goodsId = str[9];
                String goodsName = str[10];
                double stock = Double.valueOf(str[26]);
                String key = goodsId + "," + goodsName;
                stockMap.put(key, stock);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return stockMap;
    }



    /** 获取指定大仓库存，存成list
     *
     * @param filePath
     * @return
     */
    public static List<ShopStock> readWarehouseStockToList(String filePath) {
        List<ShopStock> stockList = new ArrayList<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] str = line.split(cvsSplitBy);
                String goodsId = str[9];
                String goodsName = str[10];
                String shopId = str[1];
                String date = str[0];
                double stock = Double.valueOf(str[26]);
                ShopStock shopStock = ShopStock.builder().goodsId(goodsId).shopId(shopId).date(date).shopStock(stock).build();
                stockList.add(shopStock);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return stockList;
    }

    /** 获取sku从大仓补货的所有门店列表
     *
     * @param filePath
     * @param dt
     * @param warehouseId
     * @return
     */
    public static Map<String, List<String>> readWarehouseReplGoods(String filePath, String dt, String warehouseId) {
        Map<String, List<String>> replGoodsMap = new HashMap<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] str = line.split(cvsSplitBy);
                if (!dt.equals(str[11]) || ! warehouseId.equals(str[5])) {
//                    System.out.println(line);
                    continue;
                }
                String goodsId = str[0];
                String shopId = str[1];
                String key  = warehouseId + "," + goodsId;
                List<String> shopList = replGoodsMap.get(key);
                if (shopList == null) {
                    shopList = new ArrayList<>();
                    replGoodsMap.put(key, shopList);
                }
                shopList.add(shopId);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }



        return replGoodsMap;

    }


    /** 获取sku从大仓补货的所有门店列表
     *
     * @param filePath
     * @return
     */
    public static List<SkuShopWarehouseRela> readWarehouseReplGoodsToList(String filePath) {
        List<SkuShopWarehouseRela> relations = new ArrayList<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] str = line.split(cvsSplitBy);

                String dt = str[11];
                String goodsId = str[0];
                String shopId = str[1].substring(0,4);
                String supId = str[5];
                String orderableDates = str[9];
                SkuShopWarehouseRela relation = SkuShopWarehouseRela.builder().goodsId(goodsId)
                        .shopId(shopId)
                        .supId(supId)
                        .date(dt)
                        .orderableDates(orderableDates).build();
                relations.add(relation);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return relations;

    }


    /** 读取商品列表
     *
     * @param filePath
     * @return
     */
    public static List<String> readGoodsList(String filePath) {
        List<String> goodsList = new ArrayList<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "GBK"));

            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] str = line.split(cvsSplitBy);
                String goodsId = str[0];
                goodsList.add(goodsId);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return goodsList;

    }


    /** 读取门店销量数据
     *
     * @param filePath
     * @return
     */
    public static List<Sales> readSalesToList(String filePath) {
        List<Sales> salesList = new ArrayList<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] str = line.split(cvsSplitBy);
                String shopId = str[3];
                String goodsId = str[4];
                double quantity = Double.valueOf(str[5]);
                String date = str[16];
                Sales sale = Sales.builder().goodsId(goodsId).shopId(shopId).salesQ(quantity).date(date).build();
                salesList.add(sale);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return salesList;

    }

    /** 读取门店期初期末库存，存成List
     *
     * @param filePath
     * @return
     */
    public static List<StockByShopDate> readShopStockStartEndToList(String filePath) {
        List<StockByShopDate> stockStartEndList = new ArrayList<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "GBK"));

            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] str = line.split(cvsSplitBy);
                String shopId = str[0];
                String goodsId = str[1];
                double shopEnd = Double.valueOf(str[3]);
                double amtEnd = Double.valueOf(4);
                String date = str[5];
                double shopStart = 0.0;
                double amtStart = 0.0;
                if (str.length >= 8)
                    shopStart = Double.valueOf(str[7]);
                if (str.length >= 9)
                    amtStart = Double.valueOf(str[8]);
                StockByShopDate stockByShopDate= StockByShopDate.builder().goodsId(goodsId).shopId(shopId)
                        .date(date).dateIntialStock(shopStart).dateAmtStart(amtStart)
                        .dateEndStock(shopEnd).dateAmtEnd(amtEnd).build();
                stockStartEndList.add(stockByShopDate);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return stockStartEndList;

    }


    /** 读取预测结果
     *
     * @param filePath
     * @return
     */
    public static List<PredictedSales> readPredictedSales(String filePath) {
        List<PredictedSales> salesList = new ArrayList<>();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] str = line.split(cvsSplitBy);
                String goodsId = str[0];
                int predDays = Integer.parseInt(str[1]);
                double preds = Double.valueOf(str[2]);
                String hourType = str[3];
                String shopId = str[4];
                String dt = str[6];
                PredictedSales predictedSales = new PredictedSales(goodsId, preds, dt, shopId, hourType, predDays);
                salesList.add(predictedSales);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return salesList;
    }

}
