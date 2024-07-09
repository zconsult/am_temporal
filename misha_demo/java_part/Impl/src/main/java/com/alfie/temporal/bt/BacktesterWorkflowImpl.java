package com.alfie.temporal.bt;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.Workflow;
import io.temporal.common.RetryOptions;

import io.temporal.workflow.Promise;
import io.temporal.workflow.Async;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BacktesterWorkflowImpl implements BacktesterWorkflow {
    private static final String WITHDRAW = "Withdraw";

    // RetryOptions specify how to automatically handle retries when Activities fail.
    private final RetryOptions retryoptions = RetryOptions.newBuilder()
        .setInitialInterval(Duration.ofSeconds(5)) // Wait 1 second before first retry
        .setMaximumInterval(Duration.ofSeconds(20)) // Do not exceed 20 seconds between retries
        .setBackoffCoefficient(2) // Wait 1 second, then 2, then 4, etc.
        .setMaximumAttempts(5) // Fail after 5 attempts
        .build();

    // ActivityOptions specify the limits on how long an Activity can execute before
    // being interrupted by the Orchestration service.
    private final ActivityOptions defaultActivityOptions = ActivityOptions.newBuilder()
        .setRetryOptions(retryoptions) // defined above
        .setStartToCloseTimeout(Duration.ofSeconds(20)) // Max execution time for single Activity
        .setScheduleToCloseTimeout(Duration.ofSeconds(50)) // Entire duration from scheduling to completion,
        .build();

    private final Map<String, ActivityOptions> perActivityMethodOptions = new HashMap<String, ActivityOptions>() {{
        // A heartbeat time-out is a proof-of life indicator that an activity is still working.
        // This option says to wait for 5 seconds to hear a heartbeat. If one is not heard,
        // the Activity fails.
        put(WITHDRAW, ActivityOptions.newBuilder().setHeartbeatTimeout(Duration.ofSeconds(5)).build());
    }};

    // ActivityStubs enable calls to methods as if the Activity object is local,
    // but actually perform an RPC invocation.
    private final ActivityStub accountActivityStub = Workflow.newUntypedActivityStub(defaultActivityOptions);

    // The transfer method is the entry point to the Workflow.
    // Activity method executions can be orchestrated here or from within
    // other Activity methods.
    @Override
    public void RunTest(BacktestDetails input) {
        System.out.print("test started");  System.out.flush();

        List<String> universe_list;
        try {
             universe_list = accountActivityStub.execute("load_universe", List.class, "hardcoded");
        } catch (Exception e) {
            System.out.print( "Load_universe  failed");  System.out.flush();
            return;
        }

        String dailyCloseResultPath;
        try {
            // for now all the input params are practically ignored
            dailyCloseResultPath = accountActivityStub.execute("get_daily_close", String.class,
                    universe_list, "2023-01-01", "2023-12-31");
        }
        catch (Exception e) {
            System.out.print("get_daily_close failed");  System.out.flush();
            return;
        }

        String dailyReturnsResultPath;
        try {
            dailyReturnsResultPath = accountActivityStub.execute("compute_daily_returns", String.class,
                    dailyCloseResultPath);
        }
        catch (Exception e) {
            System.out.print("compute_daily_returns failed");  System.out.flush();
            return;
        }

        List<Promise<String>> results = new ArrayList<>();

        try {
            results.add(accountActivityStub.executeAsync("load_inv_risk", String.class, dailyReturnsResultPath));
            results.add(accountActivityStub.executeAsync("compute_alpha", String.class, dailyReturnsResultPath));
            results.add(accountActivityStub.executeAsync("load_benchmarks", String.class, dailyReturnsResultPath));

            System.out.print("launched load_inv_risk compute_alpha load_benchmarks in parallel!");

            Promise.allOf(results).get();
        }
        catch (Exception e) {
            System.out.print("failed one of the first three");  System.out.flush();
            return;
        }

        results.clear();

        try {
            results.add(accountActivityStub.executeAsync("run_sim", String.class, 300));
            results.add(accountActivityStub.executeAsync("run_sim", String.class, 100));

            System.out.print("launched two run_sim in parallel!");

            Promise.allOf(results).get();
        }
        catch (Exception e) {
            System.out.print("failed ono of the last two");  System.out.flush();
            return;
        }

        // Change this from true to false to force the deposit to fail
        System.out.print("\nbacktester workflow finished\n");
        System.out.flush();
     }
}

/*
        try {
            Map<String, Object> m = accountActivityStub.execute("z_map_test", Map.class, universe_list);
            for (String key : m.keySet()) {
                System.out.println(key + ":" + m.get(key));
            }
        } catch (Exception e) {
            // If the withdrawal fails, for any exception
            System.out.printf( "Load_universe  failed");
            System.out.flush();

            // Transaction ends here
            return;
        }

 */

