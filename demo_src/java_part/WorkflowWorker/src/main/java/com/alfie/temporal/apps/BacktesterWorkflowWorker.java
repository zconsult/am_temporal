package com.alfie.temporal.apps;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

import com.alfie.temporal.bt.Shared;
import com.alfie.temporal.bt.BacktesterWorkflowImpl;
import com.alfie.temporal.bt.zConfMerger;

import org.apache.commons.cli.*;

import java.util.Map;


public class BacktesterWorkflowWorker {

    public static void main(String[] args) throws Exception {

        CommandLine cmdl = parseArgs(args);

        Map<String, Object> conf = new zConfMerger(cmdl).getConf();
        String temporalServer = (String)conf.get("temporalServer");

        // Create a stub that accesses a Temporal Service on the local development machine
        //WorkflowServiceStubs serviceStub = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowServiceStubs serviceStub =
                WorkflowServiceStubs.newServiceStubs(
                        WorkflowServiceStubsOptions.newBuilder().setTarget(temporalServer).build());


        // The Worker uses the Client to communicate with the Temporal Service
        WorkflowClient client = WorkflowClient.newInstance(serviceStub);

        // A WorkerFactory creates Workers
        WorkerFactory factory = WorkerFactory.newInstance(client);

        // A Worker listens to one Task Queue.
        // This Worker processes both Workflows and Activities
        Worker worker = factory.newWorker(Shared.BACKTESTER_TASK_QUEUE);

        // Register a Workflow implementation with this Worker
        // The implementation must be known at runtime to dispatch Workflow tasks
        // Workflows are stateful so a type is needed to create instances.
        worker.registerWorkflowImplementationTypes(BacktesterWorkflowImpl.class);

        System.out.println("Worker is running and actively polling the Task Queue.");
        System.out.println("To quit, use ^C to interrupt.");

        // Start all registered Workers. The Workers will start polling the Task Queue.
        factory.start();
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
            formatter.printHelp("BacktesterWorkflowWorker", options);
            throw e;
        }
        return cmd;
    }
}
