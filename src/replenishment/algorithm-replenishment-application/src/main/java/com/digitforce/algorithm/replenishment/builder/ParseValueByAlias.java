package com.digitforce.algorithm.replenishment.builder;

import com.digitforce.algorithm.dto.ModelParam;
import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.replenishment.component.Component;
import lombok.extern.slf4j.Slf4j;
import java.lang.reflect.Method;
import java.util.Map;

@Slf4j
public class ParseValueByAlias {

    private Map<String ,String> alias;

    private String period;

    private ReplRequest request;

    private ModelParam modelParam;

    /** 使用反射向expression中的变量传值
     *
     * @param component
     * @return
     */
    public Component parseValueByAlias(Component component) {
        for (Map.Entry<String, String> entry:alias.entrySet()) {
            String key = entry.getKey();
            String fieldName = entry.getValue();
            try {
                Object value = getFieldValueByName(fieldName, request);
                String targetName = fieldName;
                Class targetClass = request.getClass();
                if (value == null) {
                    String periodLetter = period.replaceAll("[^a-zA-Z ]", "");
                    String newFieldName = "period" + periodLetter + fieldName;
                    value = getFieldValueByName(newFieldName, request);
                    targetName = newFieldName;
                }
                if (value == null) {
                    value = getFieldValueByName(fieldName, modelParam);
                    targetClass = ModelParam.class;
                    targetName = fieldName;
                }
                if (value != null) {
                    Class<?> type = targetClass.getDeclaredField(targetName).getType();
                    if (type.isAssignableFrom(Double.class))
                        component.setVariable(key, (double) value);
                    else
                        component.setParameter(key, value);
                }

            } catch (Exception e) {
                log.error("没有找到与:{}匹配的字段值", fieldName);
            }
        }
        return component;
    }



    /**
     * 使用反射根据属性名称获取属性值
     *
     * @param fieldName 属性名称
     * @param o 操作对象
     * @return Object 属性值
     */
    private static Object getFieldValueByName(String fieldName, Object o)  {
        try {
            String firstLetter = fieldName.substring(0, 1).toUpperCase();
            String getter = "get" + firstLetter + fieldName.substring(1);
            Method method = o.getClass().getMethod(getter, new Class[] {});
            Object value = method.invoke(o, new Object[] {});
            return value;
        } catch (Exception e) {
            return null;

        }

    }



    public ParseValueByAlias(Map<String, String> alias, ReplRequest request,String period, ModelParam modelParam) {
        this.alias = alias;
        this.request= request;
        this.period = period;
        this.modelParam = modelParam;
    }
}
