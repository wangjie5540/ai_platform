import com.digitforce.algorithm.data.ReadData;
import com.digitforce.algorithm.dto.ReplRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BatchTest {


    public static void main(String[] args) {
        String path = "D:\\公司项目\\补货策略\\测试数据\\20220511_20220517.csv";
        String historyPaht = "";
        List<ReplRequest> requests = new ReadData().readReplRequest(path, "2022-05-17", new ArrayList<>(Arrays.asList("1004")));


    }
}
