package com.alfie.temporal.bt;

import java.util.Map;

public class CoreBacktestDetails implements BacktestDetails{
    private String loadUniverseArg;
    private Map<String, String> dailyCloseArgs;

    // MARK: Constructor
    public CoreBacktestDetails() {
        // Default constructor is needed for Jackson deserialization
    }

    public CoreBacktestDetails(String loadUniverseArg, Map<String, String> dailyCloseArgs){
        this.loadUniverseArg = loadUniverseArg;
        this.dailyCloseArgs  = dailyCloseArgs;
    }

    // MARK: Getter methods
    @Override
    public String getLoadUniverseActivityArg() {
        return loadUniverseArg;
    }

    @Override
    public Map<String, String> getDailyCloseActivityArgs() {
        return dailyCloseArgs;
    }
}
