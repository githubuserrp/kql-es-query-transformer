package com.rp.os.logicaloperators;

public enum LogicalOperator {
    AND(LogicalOperatorType.BINARY),
    OR(LogicalOperatorType.BINARY),
    NOT(LogicalOperatorType.UNARY);

    LogicalOperatorType type;

    private LogicalOperator(LogicalOperatorType type) {
        this.type = type;
    }

    public boolean isUnaryOperator() {
        return this.type.equals(LogicalOperatorType.UNARY);
    }

    public static boolean isUnaryOperator(String input) {
        return LogicalOperator.NOT.name().equals(input.toUpperCase());
    }

}

enum LogicalOperatorType {
    UNARY,
    BINARY
}