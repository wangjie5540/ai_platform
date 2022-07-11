import com.digitforce.algorithm.data.ReadData;
import com.digitforce.algorithm.dto.ReplRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ReadDataTest {

    public static void main(String[] args) {
        String path = "D:\\公司项目\\补货策略\\测试数据\\20220511_20220517_zbcc_new.csv";
        List<ReplRequest> requests = new ReadData().readReplRequest(path, "2022-05-17", new ArrayList<>(Arrays.asList("1004")));
//        String path = "D:\\公司项目\\补货策略\\测试数据\\大仓测试数据\\";
//        // 读取大仓库存
//        String stockPath = path + "20220511_0001大仓库存.csv";
//        Map<String, Object> warehouseStock = new ReadData().readWarehouseStock(stockPath, "20220511", "0001");
//
//        // 读取每个sku从大仓补货的门店列表
//        String replGoodsPath = path + "20220511_0001补货清单.csv";
//        Map<String, List<String>> replGoodsList = new ReadData().readWarehouseReplGoods(replGoodsPath, "20220511", "0001");


    }
}
