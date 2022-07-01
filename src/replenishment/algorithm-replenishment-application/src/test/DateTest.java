import com.digitforce.algorithm.dto.data.DateProcessor;
import org.junit.Test;

import java.util.List;

public class DateTest {
    @Test
    public void test() {
        String newDate = DateProcessor.addDate("2022-05-17", -30);
        List<String> dateList = DateProcessor.getDateList(newDate, "2022-05-17");
        System.out.println(dateList);
    }
}
