package com.alfie.temporal.bt;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.Workflow;
import io.temporal.common.RetryOptions;

import io.temporal.workflow.Promise;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BacktesterWorkflowImpl implements BacktesterWorkflow {

    // activities that run sequentially.
    // we launch them in a default task queue
    private static final String LOAD_UNIVERSE           = "load_universe";
    private static final String GET_DAILY_CLOSE         = "get_daily_close";
    private static final String COMPUTE_DAILY_RETURNS   = "compute_daily_returns";

    // activities that can run in parallel
    // we launch them in a special task queue
    private static final String LOAD_INV_RISK           = "load_inv_risk";
    private static final String COMPUTE_ALPHA           = "compute_alpha";
    private static final String LOAD_BENCHMARKS         = "load_benchmarks";
    private static final String RUN_SIM                 = "run_sim";


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

    private final ActivityOptions parallelActivityOptions = ActivityOptions.newBuilder()
            .setRetryOptions(retryoptions) // defined above
            .setStartToCloseTimeout(Duration.ofSeconds(20)) // Max execution time for single Activity
            .setScheduleToCloseTimeout(Duration.ofSeconds(50)) // Entire duration from scheduling to completion,
            .setTaskQueue(Shared.BACKTESTER_PARALLEL_TASK_QUEUE)
            .build();

    // ActivityStubs enable calls to methods as if the Activity object is local,
    // but actually perform an RPC invocation.
    private final ActivityStub defaultActivityStub = Workflow.newUntypedActivityStub(defaultActivityOptions);
    private final ActivityStub parallelActivityStub = Workflow.newUntypedActivityStub(parallelActivityOptions);

    // Activity method executions can be orchestrated here or from within
    // other Activity methods.
    @Override
    public void RunTest(BacktestDetails input) {
        System.out.print("Workflow worker code started\n");  System.out.flush();

        List<String> universe_list;
        try {
             universe_list = defaultActivityStub.execute(LOAD_UNIVERSE, List.class, input.loadUniverseActivityArg());
        } catch (Exception e) {
            System.out.print( "Load_universe  failed\n");  System.out.flush();
            return;
        }

        String dailyCloseResultPath;
        try {
            // for now all the input params are practically ignored
            dailyCloseResultPath = defaultActivityStub.execute(GET_DAILY_CLOSE, String.class,
                    universe_list, "2023-01-01", "2023-12-31");
        }
        catch (Exception e) {
            System.out.print("get_daily_close failed\n");  System.out.flush();
            return;
        }

        String dailyReturnsResultPath;
        try {
            dailyReturnsResultPath = defaultActivityStub.execute(COMPUTE_DAILY_RETURNS, String.class,
                    dailyCloseResultPath);
        }
        catch (Exception e) {
            System.out.print("compute_daily_returns failed\n");  System.out.flush();
            return;
        }

        List<Promise<String>> results = new ArrayList<>();

        try {
            results.add(parallelActivityStub.executeAsync(LOAD_INV_RISK, String.class, dailyReturnsResultPath));
            results.add(parallelActivityStub.executeAsync(COMPUTE_ALPHA, String.class, dailyReturnsResultPath));
            results.add(parallelActivityStub.executeAsync(LOAD_BENCHMARKS, String.class, dailyReturnsResultPath));

            System.out.print("\nlaunched load_inv_risk compute_alpha load_benchmarks in parallel!\n");

            Promise.allOf(results).get();
        }
        catch (Exception e) {
            System.out.print("failed one of the first three\n");  System.out.flush();
            return;
        }

        results.clear();

        try {
            results.add(parallelActivityStub.executeAsync(RUN_SIM, String.class, 300));
            results.add(parallelActivityStub.executeAsync(RUN_SIM, String.class, 100));

            System.out.print("launched two run_sim in parallel!\n");

            Promise.allOf(results).get();
        }
        catch (Exception e) {
            System.out.print("failed ono of the last two\n");  System.out.flush();
            return;
        }

        System.out.print("\nbacktester workflow finished\n");
        System.out.flush();
     }
}

