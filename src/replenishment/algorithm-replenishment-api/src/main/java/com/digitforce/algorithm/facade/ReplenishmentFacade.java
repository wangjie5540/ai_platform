package com.digitforce.algorithm.facade;

import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.dto.ReplenishmentServiceParam;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import com.digitforce.framework.api.dto.Result;

import java.util.List;

@FeignClient("algorithm-replenishment-service")
public interface ReplenishmentFacade {

    @PostMapping("/calQuantity")
    Result<List<ReplResult>> replenishmentCal (@RequestBody ReplenishmentServiceParam param);


}
