package tomlTest;

import org.junit.Test;

public class testReadConfig {

    @Test
    public void test() {
        String path = this.getClass().getClassLoader().getResource("").getPath();
        String filePath = path + "config.toml";
        System.out.println(filePath);
    }
}
