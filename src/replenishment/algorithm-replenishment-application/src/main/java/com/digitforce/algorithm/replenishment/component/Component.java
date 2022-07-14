package com.digitforce.algorithm.replenishment.component;

import com.digitforce.algorithm.dto.Formula;
import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.dto.ReplenishmentLog;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;

import java.util.*;

@Getter
@Setter
@Slf4j

/**
 * 用来描述毛需求，净需求，补货量等元素，提供元素计算逻辑，预处理，后处理等方法；
 */
public abstract class Component {
    protected String name;
    protected Component parent;
    protected Map<String, Component> subcomponents;
    protected Map<String, Double> variables;
    protected Map<String, Object> parameters; //用来存储非数值的变量，比如说列表、字符串
    protected String expression;

    private ReplResult replResult;

    private Boolean quitFlag = false;

    private ReplenishmentLog replLog;

    private String stage;

    private Map<String, Double> middleRes;

    private ReplRequest replRequest;

    public Component() {
        subcomponents = new HashMap<String, Component>();
        variables = new HashMap<String, Double>();
        parameters = new HashMap<String, Object>();
        replLog = new ReplenishmentLog();
        stage = "";
        middleRes = new HashMap<>();
        setDefaultVariables();
    }

    public void preProcess() {

    }

    public void setParent(Component parent) {
        this.parent = parent;
    }

    public double postProcess(double value) {
        return value;
    }

    protected void setDefaultVariables() {
    }

    protected void setDefaultParameters() {
    }

    public double calc() {

        for (String name : subcomponents.keySet()) {
            Component component = subcomponents.get(name);
            if (component.getParent() != null && (component.getParent().getStage() != null && !component.getParent().getStage().equals(""))) {
                String updatedPeriod = component.getStage().equals("")? component.getParent().getStage():component.getParent().getStage() + "_" + component.getStage();
                component.setStage(updatedPeriod);
            }

            double v = component.calc();
            replLog.addSubLog(component.getName(), component.getReplLog());
            if (quitFlag) {
                if (this.parent != null)
                    this.parent.setQuitFlag(quitFlag);
                return  0.0;
            }
            variables.put(name, v);
        }
        // 判断是否补货
        quitFlag = quitCal();
        if (!quitFlag) {
            preProcess();
            Expression e = new ExpressionBuilder(expression)
                    .variables(variables.keySet()).build();

            for (String v : variables.keySet())
                e.setVariable(v, variables.get(v));
//            log.info("stage{}:{}, 计算公式:{}", this.stage, this.getName(), expression);
//            log.info("参数值:{}", JSON.toJSONString(variables));
            double value = e.evaluate();

            Formula formula = new Formula(variables, expression, name, value);
            replLog.addFormula(formula);
//            log.info("计算结果:{}", value);

            double processedValue = postProcess(value);

            replLog.setName(this.getName());
            replLog.setStage(this.stage);
            return processedValue;
        } else {
            return 0.0;
        }
    }

    public void setParameter(String name, Object value) {
        this.parameters.put(name, value);
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters.putAll(parameters);
    }

    public void setVariable(String name, double value) {
        this.variables.put(name, value);
    }

    public void setVariables(Map<String, Double> variables) {
        this.variables.putAll(variables);
    }

    public double getVariable(String name) {
        return variables.get(name);
    }

    public void addSubComponent(Component component) {
        component.setParent(this);
        subcomponents.put(component.getName(), component);
    }

    public void removeSubComponent(String name) {
        if(subcomponents.containsKey(name)) {
            subcomponents.remove(name);
        }
    }

    public Component getSelectedSubComponent(String name) {
        Component component = this.getSubcomponents().get(name);
        return component;
    }

    public boolean quitCal() {
        if (this.parent != null)
            this.parent.setQuitFlag(quitFlag);
        return quitFlag;
    }


    public void processReplLog(String equation, String name, double value) {
        this.getReplLog().addFormula(variables, equation, name, value);
    }


}
