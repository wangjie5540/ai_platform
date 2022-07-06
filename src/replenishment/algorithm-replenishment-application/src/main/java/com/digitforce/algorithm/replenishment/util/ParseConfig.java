package com.digitforce.algorithm.replenishment.util;

import com.moandjiezana.toml.Toml;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ParseConfig {
    public Map<String, Object> loadMenuOrder(String name) {
        Toml toml = new Toml();
        toml.read(this.getClass().getResourceAsStream(name));
        Map<String, Object> map = toml.toMap();
        return map;
    }

    public  <T> T parseTomlConfig(Class<T> clazz, String configPath, String configName, T defaultVlaue) {
        Map<String, Object> res = loadMenuOrder(configPath);
        Map config = (Map) res.get("default");
        T value =  clazz.cast(config.getOrDefault(configName, defaultVlaue));
        return value;
    }


}
