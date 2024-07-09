package com.alfie.temporal.bt;

public class CoreBacktestDetails implements BacktestDetails{
    private String sourceAccountId;
    private String destinationAccountId;
    private String transactionReferenceId;
    private int amountToTransfer;

    // MARK: Constructor

    public CoreBacktestDetails() {
        // Default constructor is needed for Jackson deserialization
    }

    public CoreBacktestDetails(String sourceAccountId,
                                  String destinationAccountId,
                                  String transactionReferenceId,
                                  int amountToTransfer)
    {
        this.sourceAccountId = sourceAccountId;
        this.destinationAccountId = destinationAccountId;
        this.transactionReferenceId = transactionReferenceId;
        this.amountToTransfer = amountToTransfer;
    }

    // MARK: Getter methods

    public String getSourceAccountId() {
        return sourceAccountId;
    }

    public String getDestinationAccountId() {
        return destinationAccountId;
    }

    public String getTransactionReferenceId() {
        return transactionReferenceId;
    }

    public int getAmountToTransfer() {
        return amountToTransfer;
    }

}
