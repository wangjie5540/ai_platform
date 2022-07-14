package com.digitforce.algorithm.replenishment.component.demand;

public class ResidualOrderPoint extends OrderPoint{

    @Override
    public void preProcess() {

    }

    @Override
    public double postProcess(double value) {
        return value;
    }
}
