package tomlTest;

import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import org.junit.Test;

import java.util.List;

public class ParseTomlValue {

    @Test
    public void test() {
        List<Integer> orderableDatesOrder = ParseReplConfig.parseList(ParseReplConfig.orderableDatesOrderKey);
        int index = orderableDatesOrder.indexOf(4);

        System.out.println(orderableDatesOrder);
    }
}
