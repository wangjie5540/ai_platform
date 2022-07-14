package com.digitforce.algorithm.facade;

import com.digitforce.algorithm.consts.ReplProcessConsts;
import com.digitforce.algorithm.dto.*;
import com.digitforce.algorithm.replenishment.ReplenishmentService;
import com.digitforce.algorithm.replenishment.util.ParseReplConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestBody;
import com.digitforce.framework.api.dto.Result;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
public class ReplenishmentFacadeImpl implements ReplenishmentFacade{

    @Resource
    private ReplenishmentService replenishmentService;

    public Result<List<ReplResult>> replenishmentCal(@RequestBody ReplenishmentServiceParam param){
        List<ReplResult> replResults = null;
        try {
            List<ReplRequest> requests = new ArrayList<>();
            ModelParam modelParam = null;
            String date = "";
            ParentNode parentNode = null;
            Integer replProcess = ReplProcessConsts.singleStagedRepl;
            Boolean logFlag = true;
            if (param.getReplRequestList() != null && !param.getReplRequestList().isEmpty()) {
                requests = param.getReplRequestList();
            }

            if (param.getModelParam() != null) {
                modelParam = param.getModelParam();
            } else {
                // TODO 放到数据处理中
                modelParam = ParseReplConfig.getConfigModelParam();
            }

            if (param.getDate() != null && !param.getDate().isEmpty()) {
                date = param.getDate();
            }

            if (param.getReplProcess() != null) {
                replProcess = param.getReplProcess();
            }

            if (param.getParentNode() != null) {
                parentNode = param.getParentNode();
            }

            if (!param.getLogFlag()) {
                logFlag = param.getLogFlag();
            }
            replResults = replenishmentService.processRequest(requests, modelParam, date, parentNode, replProcess, logFlag);
        } catch (Exception e) {
            log.error("补货服务计算报错:{}",e);
        }
        return Result.success(replResults);
    }
}
