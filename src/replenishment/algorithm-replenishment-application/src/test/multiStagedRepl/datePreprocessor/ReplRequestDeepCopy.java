package multiStagedRepl.datePreprocessor;

import com.digitforce.algorithm.dto.ReplRequest;
import org.junit.Test;

public class ReplRequestDeepCopy {

    @Test
    public void test() {
        ReplRequest request = new ReplRequest();
        request.setDefaultValue();
        request.setSkuId("sku1");

        ReplRequest request2 = request.clone();
        request2.setSkuId("sku2");
        System.out.println(request.getSkuId() + ", " + request2.getSkuId());
    }
}
