import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
public class SQLParser {
    private final List<SQLLexer.Token> tokens;
    private int pos = 0;

    public SQLParser(List<SQLLexer.Token> tokens) {
        this.tokens = tokens;
    }

    private SQLLexer.Token peek() {
        return pos < tokens.size() ? tokens.get(pos) : null;
    }

    private SQLLexer.Token consume() {
        return tokens.get(pos++);
    }

    private boolean match(SQLLexer.TokenType type) {
        SQLLexer.Token token = peek();
        return token != null && token.type == type;
    }

    // 主解析入口
    public SQLStatement parse() {
        SQLLexer.Token token = peek();
        if (token == null) return null;

        return switch (token.type) {
            case CREATE -> parseCreateStatement();
            case USE -> parseUseStatement();
            case SHOW -> parseShowStatement();
            case INSERT -> parseInsertStatement();
            case SELECT -> parseSelectStatement();
            case UPDATE -> parseUpdateStatement();
            case DELETE -> parseDeleteStatement();
            case DROP -> parseDropStatement();
            case EXIT -> parseExitStatement();
            default -> throw new RuntimeException("无法解析的SQL语句: " + token.value);
        };
    }

    // 解析CREATE语句（DATABASE/TABLE）
    private SQLStatement parseCreateStatement() {
        consume(); // 消耗CREATE

        if (match(SQLLexer.TokenType.DATABASE)) {
            return parseCreateDatabase();
        } else if (match(SQLLexer.TokenType.TABLE)) {
            return parseCreateTable();
        }
        throw new RuntimeException("无效的CREATE语句");
    }

    private CreateDatabaseStatement parseCreateDatabase() {
        consume(); // 消耗DATABASE
        SQLLexer.Token dbName = consume();
        if (dbName.type != SQLLexer.TokenType.IDENTIFIER) {
            throw new RuntimeException("缺少数据库名称");
        }
        ensureSemicolon(); // 确保分号存在
        return new CreateDatabaseStatement(dbName.value);
    }

    private CreateTableStatement parseCreateTable() {
        consume(); // 消耗TABLE
        SQLLexer.Token tableName = consume();
        if (tableName.type != SQLLexer.TokenType.IDENTIFIER) {
            throw new RuntimeException("缺少表名称");
        }

        if (!match(SQLLexer.TokenType.LPAREN)) {
            throw new RuntimeException("缺少左括号");
        }
        consume(); // 消耗(

        List<ColumnDefinition> columns = parseColumnDefinitions();

        if (!match(SQLLexer.TokenType.RPAREN)) {
            throw new RuntimeException("缺少右括号");
        }
        consume(); // 消耗)
        ensureSemicolon(); // 确保分号存在
        return new CreateTableStatement(tableName.value, columns);
    }

    // 解析列定义列表
    private List<ColumnDefinition> parseColumnDefinitions() {
        List<ColumnDefinition> columns = new ArrayList<>();
        while (!match(SQLLexer.TokenType.RPAREN)) {
            SQLLexer.Token colName = consume();
            if (colName.type != SQLLexer.TokenType.IDENTIFIER) {
                throw new RuntimeException("缺少列名");
            }

            SQLLexer.Token typeToken = consume();
            if (!isValidDataType(typeToken.type)) {
                throw new RuntimeException("无效的数据类型: " + typeToken.value);
            }

            Integer size = parseCharSize(typeToken);
            columns.add(new ColumnDefinition(colName.value, typeToken.type, size));

            if (!match(SQLLexer.TokenType.COMMA)) break;
            consume(); // 消耗逗号
        }
        return columns;
    }

    // 解析CHAR类型的大小
    private Integer parseCharSize(SQLLexer.Token typeToken) {
        if (typeToken.type == SQLLexer.TokenType.CHAR && match(SQLLexer.TokenType.LPAREN)) {
            consume(); // 消耗(
            SQLLexer.Token sizeToken = consume();
            if (sizeToken.type != SQLLexer.TokenType.INTEGER) {
                throw new RuntimeException("CHAR类型需要指定大小");
            }
            if (!match(SQLLexer.TokenType.RPAREN)) {
                throw new RuntimeException("缺少右括号");
            }
            consume(); // 消耗)
            return Integer.parseInt(sizeToken.value);
        }
        return null;
    }

    // 解析USE语句
    private SQLStatement parseUseStatement() {
        consume(); // 消耗USE
        SQLLexer.Token dbName = consume();
        if (dbName.type != SQLLexer.TokenType.IDENTIFIER) {
            throw new RuntimeException("缺少数据库名称");
        }
        ensureSemicolon(); // 确保分号存在
        return new UseDatabaseStatement(dbName.value);
    }

    // 解析SHOW语句
    private SQLStatement parseShowStatement() {
        consume(); // 消耗SHOW
        // 检查后续Token类型
        SQLLexer.Token nextToken = peek();
        if (nextToken == null) {
            throw new RuntimeException("SHOW语句不完整");
        }

        // 处理DATABASES
        if (nextToken.type == SQLLexer.TokenType.DATABASES) {
            consume();
            return new ShowDatabasesStatement();
        }
        // 处理TABLES
        else if (nextToken.type == SQLLexer.TokenType.TABLES) {
            consume();
            return new ShowTablesStatement();
        }
        ensureSemicolon(); // 确保分号存在
        throw new RuntimeException("SHOW后必须跟TABLES或DATABASES");
    }

    // 解析INSERT语句
    private SQLStatement parseInsertStatement() {
        consume(); // 消耗INSERT
        if (!match(SQLLexer.TokenType.INTO)) {
            throw new RuntimeException("INSERT后必须跟INTO");
        }
        consume(); // 消耗INTO

        SQLLexer.Token tableName = consume();
        if (tableName.type != SQLLexer.TokenType.IDENTIFIER) {
            throw new RuntimeException("缺少表名称");
        }

        // 处理指定列名的情况（如INSERT INTO table(col1, col2)）
        List<String> columns = new ArrayList<>();
        if (match(SQLLexer.TokenType.LPAREN)) {
            consume(); // 消耗(
            while (!match(SQLLexer.TokenType.RPAREN)) {
                SQLLexer.Token col = consume();
                if (col.type != SQLLexer.TokenType.IDENTIFIER) {
                    throw new RuntimeException("缺少列名");
                }
                columns.add(col.value);
                if (!match(SQLLexer.TokenType.COMMA)) break;
                consume(); // 消耗逗号
            }
            consume(); // 消耗)
        }

        if (!match(SQLLexer.TokenType.VALUES)) {
            throw new RuntimeException("缺少VALUES关键字");
        }
        consume(); // 消耗VALUES

        if (!match(SQLLexer.TokenType.LPAREN)) {
            throw new RuntimeException("缺少左括号");
        }
        consume(); // 消耗(

        List<Expression> values = parseValueList();

        if (!match(SQLLexer.TokenType.RPAREN)) {
            throw new RuntimeException("缺少右括号");
        }
        consume(); // 消耗)

        // 新增验证：列数与值数必须匹配
        if (!columns.isEmpty() && columns.size() != values.size()) {
            throw new RuntimeException(
                String.format("列数(%d)与值数(%d)不匹配", columns.size(), values.size())
            );
        }
        ensureSemicolon(); // 确保分号存在
        return new InsertStatement(tableName.value, columns, values);
    }


    // 解析SELECT语句
    private SQLStatement parseSelectStatement() {
        // 添加调试输出
        System.out.println("Current token: " + Objects.requireNonNull(peek()).type + " -> " + Objects.requireNonNull(peek()).value);
        consume(); // SELECT
        List<Expression> columns = parseColumnList();

        if (!match(SQLLexer.TokenType.FROM)) {
            throw new RuntimeException("缺少FROM关键字");
        }
        consume();

        List<TableReference> tables = parseTableList();
        Expression where;
        if(match(SQLLexer.TokenType.WHERE)) {
            consume();
            where =parseCondition();
        }else{
            where =null;
        }
        ensureSemicolon(); // 确保分号存在
        return new SelectStatement(columns, tables, where);
    }

    // 解析UPDATE语句
    private SQLStatement parseUpdateStatement() {
        consume(); // 消耗UPDATE
        //解析表名
        SQLLexer.Token tableToken = consume();
        if (tableToken.type != SQLLexer.TokenType.IDENTIFIER) {
            throw new RuntimeException("缺少表名称");
        }
        String tableName = tableToken.value;

        // 解析SET子句
        if (!match(SQLLexer.TokenType.SET)) {
            throw new RuntimeException("缺少SET关键字");
        }
        consume(); // 消耗SET

        List<Assignment> assignments = new ArrayList<>();
        do {
            // 解析列名
            SQLLexer.Token columnToken = consume();
            if (columnToken.type != SQLLexer.TokenType.IDENTIFIER) {
                throw new RuntimeException("缺少列名");
            }

            // 解析等号
            if (!match(SQLLexer.TokenType.EQ)) {
                throw new RuntimeException("缺少等号");
            }
            consume(); // 消耗=

            // 解析值表达式
            Expression value = parseExpression();
            assignments.add(new Assignment(columnToken.value, value));

            // 检查是否有更多赋值
            if (!match(SQLLexer.TokenType.COMMA)) break;
            consume(); // 消耗逗号
        } while (true);

            // 解析可选的WHERE子句
            Expression where = null;
            if (match(SQLLexer.TokenType.WHERE)) {
                consume(); // 消耗WHERE
                where = parseCondition();
            }

            ensureSemicolon(); // 确保分号存在
            return new UpdateStatement(tableName, assignments, where);
        }

    // 解析DELETE语句
    private SQLStatement parseDeleteStatement() {
        consume(); // 消耗DELETE
        if (!match(SQLLexer.TokenType.FROM)) {
            throw new RuntimeException("DELETE后必须跟FROM");
        }
        consume(); // 消耗FROM
        //解析表名
        SQLLexer.Token tableName = consume();
        if (tableName.type != SQLLexer.TokenType.IDENTIFIER) {
            throw new RuntimeException("缺少表名称");
        }
        //解析WHERE条件
        Expression where = null;
        if (match(SQLLexer.TokenType.WHERE)) {
            consume(); // 消耗WHERE
            where = parseCondition();
        }
        ensureSemicolon(); // 确保分号存在
        return new DeleteStatement(tableName.value, where);
    }

    // 解析DROP语句
    private SQLStatement parseDropStatement() {
        consume(); // 消耗DROP

        if (match(SQLLexer.TokenType.DATABASE)) {
            consume(); // 消耗DATABASE
            SQLLexer.Token dbName = consume();
            if (dbName.type != SQLLexer.TokenType.IDENTIFIER) {
                throw new RuntimeException("缺少数据库名称");
            }
            return new DropDatabaseStatement(dbName.value);
        } else if (match(SQLLexer.TokenType.TABLE)) {
            consume(); // 消耗TABLE
            SQLLexer.Token tableName = consume();
            if (tableName.type != SQLLexer.TokenType.IDENTIFIER) {
                throw new RuntimeException("缺少表名称");
            }
            return new DropTableStatement(tableName.value);
        }
        ensureSemicolon(); // 确保分号存在
        throw new RuntimeException("无效的DROP语句");
    }

    // 解析EXIT语句
    private SQLStatement parseExitStatement() {
        consume(); // 消耗EXIT
        ensureSemicolon(); // 确保分号存在
        return new ExitStatement();
    }

    // 解析条件表达式（WHERE子句）
    private Expression parseCondition() {
    return parseOrExpression(); // 从最低优先级运算符开始解析
}

    // 解析OR条件（最低优先级）
    private Expression parseOrExpression() {
        Expression left = parseAndExpression();
        while (match(SQLLexer.TokenType.OR)) {
            SQLLexer.Token op = consume();
            Expression right = parseAndExpression();
            left = new LogicalExpression(left, op.type, right);
        }
        return left;
    }

    // 解析AND条件（中等优先级）
    private Expression parseAndExpression() {
        Expression left = parsePrimaryCondition();
        while (match(SQLLexer.TokenType.AND)) {
            SQLLexer.Token op = consume();
            Expression right = parsePrimaryCondition();
            left = new LogicalExpression(left, op.type, right);
        }
        return left;
    }

    // 解析基础条件（最高优先级）
    private Expression parsePrimaryCondition() {
        // 处理括号分组
        if (match(SQLLexer.TokenType.LPAREN)) {
            consume();
            Expression expr = parseCondition();
            if (!match(SQLLexer.TokenType.RPAREN)) {
                throw new RuntimeException("缺少右括号");
            }
            consume();
            return expr;
        }

        // NOT运算符优化处理
        if (match(SQLLexer.TokenType.NOT)) {
            consume();
            Expression innerExpr = parsePrimaryCondition();
            if (innerExpr instanceof IsNullExpression expr) {
                return new IsNullExpression(expr.getOperand(), !expr.isNot());
            }
            return new NotExpression();
        }

        // 处理特殊运算符（BETWEEN/IN/LIKE等）
        Expression left = parseExpression();
        SQLLexer.Token op = peek();

         // IS NULL/NOT NULL处理
        if (match(SQLLexer.TokenType.IS)) {
            consume();
            boolean isNotNull = match(SQLLexer.TokenType.NOT) && (consume() != null);
            if (!match(SQLLexer.TokenType.NULL)) {
                throw new RuntimeException("IS后必须跟NULL或NOT NULL");
            }
            consume();
            return new IsNullExpression(left, isNotNull);
        }

        // BETWEEN A AND B
        if (match(SQLLexer.TokenType.BETWEEN)) {
            consume();
            Expression start = parseExpression();
            if (!match(SQLLexer.TokenType.AND)) {
                throw new RuntimeException("BETWEEN必须配合AND使用");
            }
            consume();
            Expression end = parseExpression();
            return new BetweenExpression(left, start, end);
        }

        // 标准比较运算符
        if (op != null && isComparisonOperator(op.type)) {
            consume();
            Expression right = parseExpression();
            return new ComparisonExpression(left, op.type, right);
        }

        throw new RuntimeException("无效的条件表达式");
    }

    // 解析表达式（列引用、常量或函数调用）
    private Expression parseExpression() {
        SQLLexer.Token token = consume();
        return switch (token.type) {
            case NULL -> new ConstantExpression(null);
            case IDENTIFIER -> {
                if (match(SQLLexer.TokenType.DOT)) {
                    consume(); // 消耗点号
                    SQLLexer.Token columnToken = consume();
                    yield new QualifiedColumnReference();
                }
                yield new ColumnReference(token.value);
                // 处理带表名前缀的列引用（如student.id）
                // 消耗点号
            }
            case INTEGER -> new ConstantExpression(Integer.parseInt(token.value));
            case FLOAT -> new ConstantExpression(Double.parseDouble(token.value));
            case STRING -> new ConstantExpression(token.value);
            case STAR -> new AllColumnsExpression();
            default -> throw new RuntimeException("无效的表达式: " + token.value);
        };
    }

    // 增强解析值列表（INSERT VALUES (val1, val2)）
    private List<Expression> parseValueList() {
        List<Expression> values = new ArrayList<>();
        while (!match(SQLLexer.TokenType.RPAREN)) {
            // 处理NULL值
            if (match(SQLLexer.TokenType.NULL)) {
                values.add(new ConstantExpression(null));
                consume();
            } else {
                values.add(parseExpression());
            }

            // 确保正确处理逗号或右括号
            if (!match(SQLLexer.TokenType.COMMA)) {
                if (!match(SQLLexer.TokenType.RPAREN)) {
                    throw new RuntimeException("VALUES列表缺少逗号或右括号");
                }
                break;
            }
            consume(); // 消耗逗号
        }
        return values;
    }


    // 解析列列表（SELECT col1, col2）
    private List<Expression> parseColumnList() {
        List<Expression> columns = new ArrayList<>();
        if (match(SQLLexer.TokenType.STAR)) {
            consume(); // 消耗*
            columns.add(new AllColumnsExpression());
        } else {
            do {
                columns.add(parseExpression());
                if (!match(SQLLexer.TokenType.COMMA)) break;
                consume(); // 消耗逗号
            } while (true);
        }
        return columns;
    }

    // 解析表列表（FROM table1, table2）
    private List<TableReference> parseTableList() {
        List<TableReference> tables = new ArrayList<>();
        while (!match(SQLLexer.TokenType.WHERE) && !match(SQLLexer.TokenType.EOF)) {
            // 解析表名
            SQLLexer.Token tableToken = consume();
            if (tableToken.type != SQLLexer.TokenType.IDENTIFIER) {
                throw new RuntimeException("缺少表名: " + tableToken.value);
            }

            // 解析别名
            String alias = tableToken.value; // 默认别名=表名
            if (match(SQLLexer.TokenType.IDENTIFIER)) {
                alias = consume().value; // 显式指定别名
            }

            tables.add(new TableReference(tableToken.value, alias));

            // 处理逗号分隔
            if (!match(SQLLexer.TokenType.COMMA)) break;
            consume();
        }
        return tables;
    }

    // 辅助方法：验证数据类型是否有效
    private boolean isValidDataType(SQLLexer.TokenType type) {
        return type == SQLLexer.TokenType.INT ||
                type == SQLLexer.TokenType.CHAR ||
                type == SQLLexer.TokenType.VARCHAR ||
                type == SQLLexer.TokenType.FLOAT;
    }
    // 新增辅助方法，分号作为结束符
    private void ensureSemicolon() {
        if (!match(SQLLexer.TokenType.SEMICOLON)) {
            throw new RuntimeException("缺少语句结束符分号(;)");
        }
        consume(); // 消耗分号
    }
    // 辅助方法：验证是否为比较运算符
    private boolean isComparisonOperator(SQLLexer.TokenType type) {
        return type == SQLLexer.TokenType.EQ ||
           type == SQLLexer.TokenType.NEQ ||
           type == SQLLexer.TokenType.GT ||
           type == SQLLexer.TokenType.LT ||
           type == SQLLexer.TokenType.GTE ||
           type == SQLLexer.TokenType.LTE;
        // 注意：这里不包含DOT类型
    }

    // ========== AST节点类定义 ==========

    // SQL语句基类
    public abstract static class SQLStatement {
        public enum Type {
            CREATE_DATABASE, CREATE_TABLE, USE_DATABASE,
            SHOW_TABLES, SHOW_DATABASES,
            INSERT, SELECT, UPDATE, DELETE,
            DROP_TABLE, DROP_DATABASE, EXIT
        }

        public abstract Type getType();
    }

    // 具体语句类
    public static class CreateDatabaseStatement extends SQLStatement {
        private final String dbName;

        public CreateDatabaseStatement(String dbName) {
            this.dbName = dbName;
        }

        @Override
        public Type getType() {
            return Type.CREATE_DATABASE;
        }

        public String getDbName() {
            return dbName;
        }
    }


    // BETWEEN表达式
    public static class BetweenExpression implements Expression {
        private final Expression value;
        private final Expression start;
        private final Expression end;

        public Expression getValue() {
            return value;
        }

        public Expression getStart() {
            return start;
        }

        public Expression getEnd() {
            return end;
        }

        public BetweenExpression(Expression value, Expression start, Expression end) {
            this.value = value;
            this.start = start;
            this.end = end;
        }
    }

    // IS NULL表达式
   public static class IsNullExpression implements Expression {
    private final Expression operand;  // 建议命名为operand或left，保持一致性
    private final boolean isNot;

    public IsNullExpression(Expression operand, boolean isNot) {
        this.operand = Objects.requireNonNull(operand);
        this.isNot = isNot;
    }

    // 明确定义Getter方法
    public Expression getOperand() {
        return operand;
    }
    public boolean isNot() {
        return isNot;
    }
}

    // NOT表达式
    public static class NotExpression implements Expression {

        public NotExpression() {
        }
        // 构造方法 & Getter
    }
    public static class CreateTableStatement extends SQLStatement {
        private final String tableName;
        private final List<ColumnDefinition> columns;

        public CreateTableStatement(String tableName, List<ColumnDefinition> columns) {
            this.tableName = tableName;
            this.columns = columns;
        }

        @Override
        public Type getType() {
            return Type.CREATE_TABLE;
        }

        public String getTableName() {
            return tableName;
        }

        public List<ColumnDefinition> getColumns() {
            return columns;
        }
    }

    public static class UseDatabaseStatement extends SQLStatement {
        private final String dbName;

        public UseDatabaseStatement(String dbName) {
            this.dbName = dbName;
        }

        @Override
        public Type getType() {
            return Type.USE_DATABASE;
        }

        public String getDbName() {
            return dbName;
        }
    }

    public static class ShowTablesStatement extends SQLStatement {
        @Override
        public Type getType() {
            return Type.SHOW_TABLES;
        }
    }

    public static class ShowDatabasesStatement extends SQLStatement {
        @Override
        public Type getType() {
            return Type.SHOW_DATABASES;
        }
    }

    public static class InsertStatement extends SQLStatement {
        private final String tableName;
        private final List<String> columns;
        private final List<Expression> values;

        public InsertStatement(String tableName, List<String> columns, List<Expression> values) {
            this.tableName = tableName;
            this.columns = columns;
            this.values = values;
        }

        @Override
        public Type getType() {
            return Type.INSERT;
        }

        public String getTableName() {
            return tableName;
        }

        public List<String> getColumns() {
            return columns;
        }

        public List<Expression> getValues() {
            return values;
        }
    }

    public static class SelectStatement extends SQLStatement {
        private final List<Expression> columns;
        private final List<TableReference> tables;
        private final Expression where;

        public SelectStatement(List<Expression> columns, List<TableReference> tables, Expression where) {
            this.columns = columns;
            this.tables = tables;
            this.where = where;
        }

        // 新增方法


        @Override
        public Type getType() {
            return Type.SELECT;
        }

        public List<Expression> getColumns() {
            return columns;
        }

        public List<TableReference> getTables() {
            return tables;
        }

        public Expression getWhere() {
            return where;
        }
    }

    public static class UpdateStatement extends SQLStatement {
        private final String tableName;
        private final List<Assignment> assignments;
        private final Expression where;

        public UpdateStatement(String tableName, List<Assignment> assignments, Expression where) {
            this.tableName = tableName;
            this.assignments = assignments;
            this.where = where;
        }

        @Override
        public Type getType() {
            return Type.UPDATE;
        }

        public String getTableName() {
            return tableName;
        }

        public List<Assignment> getAssignments() {
            return assignments;
        }

        public Expression getWhere() {
            return where;
        }
    }

    public static class DeleteStatement extends SQLStatement {
        private final String tableName;
        private final Expression where;

        public DeleteStatement(String tableName, Expression where) {
            this.tableName = tableName;
            this.where = where;
        }

        @Override
        public Type getType() {
            return Type.DELETE;
        }

        public String getTableName() {
            return tableName;
        }

        public Expression getWhere() {
            return where;
        }
    }

    public static class DropTableStatement extends SQLStatement {
        private final String tableName;

        public DropTableStatement(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public Type getType() {
            return Type.DROP_TABLE;
        }

        public String getTableName() {
            return tableName;
        }
    }

    public static class DropDatabaseStatement extends SQLStatement {
        private final String dbName;

        public DropDatabaseStatement(String dbName) {
            this.dbName = dbName;
        }

        @Override
        public Type getType() {
            return Type.DROP_DATABASE;
        }

        public String getDbName() {
            return dbName;
        }
    }

    public static class ExitStatement extends SQLStatement {
        @Override
        public Type getType() {
            return Type.EXIT;
        }
    }

    // 表达式相关类
    public interface Expression {
    }

    public static class ColumnReference implements Expression {
        private final String columnName;

        public ColumnReference(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public String toString() {
            return columnName; // 输出列名，如"name"
        }

        public String getColumnName() {
            return columnName;
        }
    }

    public static class AllColumnsExpression implements Expression {
    }

    public static class ConstantExpression implements Expression {
        private final Object value;

        public ConstantExpression(Object value) {
            this.value = value;
        }


        @Override
        public String toString() {
            return String.valueOf(value); // 输出实际值，如1、"Alice"
        }

        public Object getValue() {
            return value;
        }
    }

    //存储表名和别名的映射关系
    public static class TableReference {
        private final String tableName;
        private final String alias;

        public TableReference(String tableName, String alias) {
            this.tableName = tableName;
            this.alias = alias;
        }

        // Getters
        public String getTableName() {
            return tableName;
        }

        @Override
        public String toString() {
            return alias.equals(tableName) ? tableName : tableName + " AS " + alias;
        }

        public String getName() {
            return tableName;
        }
    }

    public static class ComparisonExpression implements Expression {
        private final Expression left;
        private final SQLLexer.TokenType operator;
        private final Expression right;

        public ComparisonExpression(Expression left, SQLLexer.TokenType operator, Expression right) {
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        public Expression getLeft() {
            return left;
        }

        public SQLLexer.TokenType getOperator() {
            return operator;
        }

        public Expression getRight() {
            return right;
        }
    }

    public static class LogicalExpression implements Expression {
        private final Expression left;
        private final SQLLexer.TokenType operator;
        private final Expression right;

        public LogicalExpression(Expression left, SQLLexer.TokenType operator, Expression right) {
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        public Expression getLeft() {
            return left;
        }

        public SQLLexer.TokenType getOperator() {
            return operator;
        }

        public Expression getRight() {
            return right;
        }
    }

    public static class Assignment {
        private final String column;
        private final Expression value;

        public Assignment(String column, Expression value) {
            this.column = column;
            this.value = value;
        }

        public String getColumn() {
            return column;
        }

        public Expression getValue() {
            return value;
        }
    }

    // 列定义类
    public static class ColumnDefinition {
        private final String name;
        private final SQLLexer.TokenType type;
        private final Integer size; // 用于CHAR(N)

        public ColumnDefinition(String name, SQLLexer.TokenType type, Integer size) {
            this.name = name;
            this.type = type;
            this.size = size;
        }

        public String getName() {
            return name;
        }

        public SQLLexer.TokenType getType() {
            return type;
        }

        public Integer getSize() {
            return size;
        }
    }

    // 带表名前缀的列引用
    public static class QualifiedColumnReference implements Expression {

        public QualifiedColumnReference() {
        }

    }


    //测试
    public static void main(String[] args) {
        // 测试用例集合
        String[] testSQLs = {
                // DDL测试
                "CREATE DATABASE school",
                "CREATE TABLE students (id INT, name VARCHAR(50), age INT)",

                // DML测试
                "INSERT INTO students VALUES (1, 'Alice', 20)",
                "UPDATE students SET age = 21 WHERE id = 1",
                "DELETE FROM students WHERE age > 25",

                // 查询测试
                "SELECT * FROM students",
                "SELECT name, age FROM students WHERE id = 1",
                "SELECT s.name, c.course FROM students s, courses c WHERE s.id = c.student_id",

                // 错误SQL测试
                "CREATE TABLE (id INT)",  // 缺少表名
                "SELECT FROM students"    // 缺少列名
        };

        // 执行测试
        for (String sql : testSQLs) {
            System.out.println("===== 测试SQL: " + sql + " =====");
            try {
                // 词法分析
                SQLLexer lexer = new SQLLexer(sql);
                List<SQLLexer.Token> tokens = lexer.tokenize();

                // 语法分析
                SQLParser parser = new SQLParser(tokens);
                SQLParser.SQLStatement statement = parser.parse();

                // 输出解析结果
                printStatementInfo(statement);
            } catch (Exception e) {
                System.err.println("解析错误: " + e.getMessage());
            }
            System.out.println();
        }
    }
    // 在SQLParser类中添加以下内部类
    public static class BinaryExpression implements Expression {
        private final Expression left;
        private final Expression right;
        private final SQLLexer.TokenType operator;

        public BinaryExpression(Expression left, SQLLexer.TokenType operator, Expression right) {
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        public Expression getLeft() {
            return left;
        }

        public Expression getRight() {
            return right;
        }

        public SQLLexer.TokenType getOperator() {
            return operator;
        }

        @Override
        public String toString() {
            String opSymbol = switch (operator) {
                case PLUS -> "+";
                case MINUS -> "-";
                case MULTIPLY -> "*";
                case DIVIDE -> "/";
                case EQ -> "=";
                case NEQ -> "!=";
                case LT -> "<";
                case GT -> ">";
                case LTE -> "<=";
                case GTE -> ">=";
                case AND -> "AND";
                case OR -> "OR";
                default -> operator.toString();
            };
            return "(" + left + " " + opSymbol + " " + right + ")";
        }
    }
    private static void printStatementInfo(SQLParser.SQLStatement statement) {
        if (statement == null) {
            System.out.println("空语句");
            return;
        }

        switch (statement.getType()) {
            case CREATE_DATABASE:
                SQLParser.CreateDatabaseStatement createDb = (SQLParser.CreateDatabaseStatement) statement;
                System.out.println("[CREATE DATABASE] 数据库名: " + createDb.getDbName());
                break;

            case CREATE_TABLE:
                SQLParser.CreateTableStatement createTable = (SQLParser.CreateTableStatement) statement;
                System.out.println("[CREATE TABLE] 表名: " + createTable.getTableName());
                System.out.println("列定义:");
                createTable.getColumns().forEach(col ->
                        System.out.printf("  %s %s%s%n",
                                col.getName(),
                                col.getType(),
                                col.getSize() != null ? "(" + col.getSize() + ")" : "")
                );
                break;

            case INSERT:
                SQLParser.InsertStatement insert = (SQLParser.InsertStatement) statement;
                System.out.println("[INSERT] 表名: " + insert.getTableName());
                if (!insert.getColumns().isEmpty()) {
                    System.out.println("指定列: " + String.join(", ", insert.getColumns()));
                }
                System.out.println("值列表:");
                insert.getValues().forEach(val -> System.out.println("  " + val));
                break;

            case SELECT:
                SQLParser.SelectStatement select = (SQLParser.SelectStatement) statement;
                System.out.println("[SELECT]");
                System.out.println("查询列: " + select.getColumns());
                System.out.println("数据源: " + String.join(", ", select.getTables().getFirst().getTableName()));
                if (select.getWhere() != null) {
                    System.out.println("条件: " + select.getWhere());
                }
                break;

            case UPDATE:
                SQLParser.UpdateStatement update = (SQLParser.UpdateStatement) statement;
                System.out.println("[UPDATE] 表名: " + update.getTableName());
                System.out.println("赋值操作:");
                update.getAssignments().forEach(assign ->
                        System.out.printf("  %s = %s%n", assign.getColumn(), assign.getValue())
                );
                if (update.getWhere() != null) {
                    System.out.println("条件: " + update.getWhere());
                }
                break;

            case DELETE:
                SQLParser.DeleteStatement delete = (SQLParser.DeleteStatement) statement;
                System.out.println("[DELETE] 表名: " + delete.getTableName());
                if (delete.getWhere() != null) {
                    System.out.println("条件: " + delete.getWhere());
                }
                break;

            default:
                System.out.println("其他语句类型: " + statement.getType());
        }
    }

}