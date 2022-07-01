package com.digitforce.algorithm.replenishment.component.compoundComponent;

import com.digitforce.algorithm.dto.ReplRequest;
import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.replenishment.component.Component;

import java.util.ArrayList;
import java.util.List;

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

    //
//    @Override
//    protected void setDefaultVariables() {
//        super.setDefaultVariables();
//        setVariable("上级库存量", 0.0);
//    }
//
//    public ReplRatioReallocate() {
//        super();
//        setName(name);
//    }
//
//    @Override
//    protected void setDefaultParameters() {
//        super.setDefaultParameters();
//        setParameter("单个商品补货结果列表", new ArrayList<ReplResult>());
//    }
//
//
//    /** 判断是否要重新分配补货量；
//     *
//     * @return
//     */
//    private boolean isReallocateOrNot() {
//        List<ReplResult> resultForSku = (List<ReplResult>) getParameters().get("单个商品补货结果列表");
//        double parentStock = getVariable("上级库存量");
//        double totalReplQuant = resultForSku.stream().mapToDouble(e->e.getReplQuant()).sum();
//        if (parentStock < totalReplQuant)
//            return  true;
//        return false;
//    }
//
//
//    @Override
//    public void preProcess() {
//        boolean reAllocateFlag = isReallocateOrNot();
//        if (reAllocateFlag) {
//            List<ReplResult> resultForSku = (List<ReplResult>) getParameters().get("单个商品补货结果列表");
//            double totalReplQuant = resultForSku.stream().mapToDouble(e -> e.getReplQuant()).sum();
//            double parentStock = getVariable("上级库存量");
//            for (ReplResult result : resultForSku) {
//                double replQuant = result.getReplQuant();
//                double reAllocateQuant = replQuant / totalReplQuant * parentStock;
//                result.setReplQuant(reAllocateQuant);
//            }
//        }
//    }
}
