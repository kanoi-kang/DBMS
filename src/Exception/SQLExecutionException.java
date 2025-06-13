package Exception;

public class SQLExecutionException extends RuntimeException {
    private final String sqlState;

    public SQLExecutionException(String message) {
        this(message, "42000");
    }

    public SQLExecutionException(String message, String sqlState) {
        super(message);
        this.sqlState = sqlState;
    }

    // 兼容MySQL错误代码[7](@ref)
    public static SQLExecutionException forTableNotFound(String tableName) {
        return new SQLExecutionException(
                "Table '" + tableName + "' doesn't exist",
                "42S02"); // MySQL表不存在错误码
    }
}