package com.alfie.temporal.apps;

import com.alfie.temporal.bt.BacktestDetails;
import com.alfie.temporal.bt.zConfMerger;
import com.alfie.temporal.bt.Shared;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;

import org.apache.commons.cli.*;

import java.util.Map;


public class LauncherApp {

    public static void main(String[] args) throws Exception {

        LauncherApp lapp = new LauncherApp(args);

        // Perform asynchronous execution.
        // This process exits after making this call and printing details.
        lapp.launch();

        System.exit(0);
    }

    private static CommandLine parseArgs(String[] args) throws Exception {
        Options options = new Options();
        Option conf = new Option("conf", "configFile", true,
                "Full path to the user config file");
        conf.setRequired(false);
        options.addOption(conf);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter  = new HelpFormatter();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        }
        catch (org.apache.commons.cli.ParseException e) {
            formatter.printHelp("Launcher", options);
            throw e;
        }
        return cmd;
    }

    private LauncherApp(String[] args) throws Exception {
        CommandLine cmdl = parseArgs(args);
        Map<String, Object> conf = new zConfMerger(cmdl).getConf();

        String temporalServer = (String)conf.get("temporalServer");
        String workflowID = (String)conf.get("workflowID");
        String workflowType = (String)conf.get("workflowType");

        // A WorkflowServiceStubs communicates with the Temporal front-end service.
        WorkflowServiceStubs serviceStub =
                WorkflowServiceStubs.newServiceStubs(
                        WorkflowServiceStubsOptions.newBuilder().setTarget(temporalServer).build());

        // A WorkflowClient wraps the stub.
        // It can be used to start, signal, query, cancel, and terminate Workflows.
        WorkflowClient client = WorkflowClient.newInstance(serviceStub);

        // Workflow options configure  Workflow stubs.
        // A WorkflowId prevents duplicate instances, which are removed.
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue(Shared.BACKTESTER_TASK_QUEUE)
                //            .setWorkflowExecutionTimeout(Duration.ofSeconds(50))
                //            .setWorkflowRunTimeout(Duration.ofSeconds(50))
                .setWorkflowId(workflowID)
                .build();

        // WorkflowStubs enable calls to methods as if the Workflow object is local
        // but actually perform a gRPC call to the Temporal Service.
        this.workflow =  client.newUntypedWorkflowStub(workflowType, options);

        // Prepare some (mostly ignored:)) arguments
        String loadUniverseArg = (String)conf.get("loadUniverseArg");
        this.bd = new BacktestDetails(loadUniverseArg, Map.of("key1", "value1"));
    }

    private void launch() {
        WorkflowExecution we = workflow.start(bd);

        System.out.print("\nBACKTESTER PROJECT\n\n");
        System.out.printf("[WorkflowID: %s]\n[RunID: %s]\n\n", we.getWorkflowId(), we.getRunId());
    }

    private final WorkflowStub workflow;
    private final BacktestDetails bd;
}
