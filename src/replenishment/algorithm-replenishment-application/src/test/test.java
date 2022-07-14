import com.alibaba.fastjson.JSON;
import com.digitforce.algorithm.dto.ReplRequest;

public class test {

    public static void main(String[] args) {
        String a= "";
        System.out.println(a.isEmpty());

        ReplRequest request = new ReplRequest();
        request.setDefaultValue();
        System.out.println(JSON.toJSON(request));
        System.out.println(request.getMinInStock() == 0.0);
        System.out.println(Double.compare(request.getMinInStock(), 0.0) == 0);

        request.setIsNew(true);
        ReplRequest request1 = new ReplRequest();

        if (request.getIsNew()) {
            request1.setIsNew(request.getIsNew());
        }
        System.out.println(JSON.toJSON(request1));
    }
}
