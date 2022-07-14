//package multiStagedRepl.datePreprocessor;
//
//import com.digitforce.algorithm.data.DataPreprocessor;
//import com.digitforce.algorithm.data.ReadData;
//import com.digitforce.algorithm.dto.ReplRequest;
//import org.junit.Test;
//
//import java.util.List;
//import java.util.stream.Collectors;
//
//public class DataPreprocessorTest {
//
//    @Test
//    public  void test() {
//        List<ReplRequest> requests = new DataPreprocessor().generateReplList();
//        System.out.println(requests.stream().map(e->e.getSkuId()).collect(Collectors.toList()));
//
//        String paramGoods = "D:\\公司项目\\补货策略\\测试数据\\20220511有补货参数数据的goods_id.csv";
//        List<String> readGoodsList = ReadData.readGoodsList(paramGoods);
//        List<String> leftIds = requests.stream().filter(e->!readGoodsList.contains(e.getSkuId())).map(ee->ee.getSkuId()).collect(Collectors.toList());
//        System.out.println(leftIds);
//        System.out.println("finish!");
//    }
//}
