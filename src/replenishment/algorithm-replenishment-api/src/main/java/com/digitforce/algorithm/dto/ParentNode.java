package com.digitforce.algorithm.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class ParentNode {
    private String parent;

    private Map<String, Double> parentStock;
}
