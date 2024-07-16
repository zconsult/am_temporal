package com.alfie.temporal.bt;

import java.util.Map;

public record  BacktestDetails (
    String loadUniverseActivityArg,
    Map<String, String> dailyCloseActivityArgs
) {}


