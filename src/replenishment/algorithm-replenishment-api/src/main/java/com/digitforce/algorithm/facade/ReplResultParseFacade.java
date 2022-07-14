package com.digitforce.algorithm.facade;

import com.digitforce.algorithm.dto.ReplResult;
import com.digitforce.algorithm.dto.ReplenishmentServiceParam;
import com.digitforce.framework.api.dto.Result;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;

public interface ReplResultParseFacade {


    @PostMapping("/parseResult")
    Result replParseResult (@RequestBody ReplResult result);

}
