package com.digitforce.algorithm.replenishment.component.compoundComponent;

import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.replenishment.component.Component;


public class ReplRatioReallocate extends Component {

    String name = "分配补货量";

    String expression = "补货量 / 总补货量 * 上级库存量";

        @Override
        protected void setDefaultVariables() {
            super.setDefaultVariables();
            setVariable("上级库存量", 0.0);
            setVariable("总补货量", 0.0);
        }



        public ReplRatioReallocate() {
            super();
            setName(name);
            setExpression(expression);
        }

        @Override
        protected void setDefaultParameters() {
            super.setDefaultParameters();
            setParameter("补货结果", new ReplResult());
        }

    @Override
    public void preProcess() {
        ReplResult result = (ReplResult) getParameters().get("补货结果");
        setVariable("补货量", result.getReplQuant());
    }

}
