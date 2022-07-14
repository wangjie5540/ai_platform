import com.moandjiezana.toml.Toml;

import java.util.Map;

public class testClass {

    public Map<String, Object> loadMenuOrder(String name) {
        Toml toml = new Toml();
        toml.read(this.getClass().getResourceAsStream("config.toml"));
        Map<String, Object> map = toml.toMap();
        return map;

    }

}
