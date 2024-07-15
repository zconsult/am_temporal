package com.alfie.temporal.bt;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Map;

@JsonDeserialize(as = CoreBacktestDetails.class)
public interface BacktestDetails {
    String getLoadUniverseActivityArg();
    Map<String, String> getDailyCloseActivityArgs();
}

