import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class SQLExecutor {
    private String currentDatabase;
    private final Map<String, Database> databases = new HashMap<>();
    private final String baseDir = "data";

    // 初始化数据目录并加载已有数据库
    public SQLExecutor() {
        initDataDirectory();
        loadExistingDatabases();
    }

    private void initDataDirectory() {
        try {
            Files.createDirectories(Paths.get(baseDir));
        } catch (IOException e) {
            throw new RuntimeException("无法创建数据目录: " + e.getMessage());
        }
    }

    private void loadExistingDatabases() {
    File dataDir = new File(baseDir);
    if (dataDir.exists() && dataDir.isDirectory()) {
        File[] dbDirs = dataDir.listFiles(File::isDirectory);
        if (dbDirs != null) {
            for (File dbDir : dbDirs) {
                try {
                    loadDatabase(dbDir.getName());
                } catch (IOException e) {
                    System.err.println("加载数据库失败: " + dbDir.getName() + ", 原因: " + e.getMessage());
                }
            }
        }
    }
}

    private void loadDatabase(String name) throws IOException {
        Path dbPath = Paths.get(baseDir, name);
        Database db = new Database(name);

        File[] tableFiles = dbPath.toFile().listFiles(f ->
                f.getName().endsWith(".tbl") && f.canRead()
        );

        if (tableFiles != null) {
            for (File tableFile : tableFiles) {
                String tableName = tableFile.getName().replace(".tbl", "");
                db.addTable(loadTable(tableName, tableFile));
            }
        }
        databases.put(name, db);
    }

    private Table loadTable(String name, File tableFile) throws IOException {
        Path tablePath = tableFile.toPath(); // 转换为Path对象
        List<String> lines = Files.readAllLines(tablePath,StandardCharsets.UTF_8);
        if (lines.isEmpty()) throw new IOException("空表文件: " + tableFile.getName());

        // 修复列定义解析
        String header = lines.getFirst();
        String[] colDefs = header.split("\\|");
        Table table = new Table(name);

        for (String colDef : colDefs) {
            String[] parts = colDef.split(":");
            if (parts.length < 2) continue;
            String colName = parts[0].toUpperCase();
            SQLLexer.TokenType colType = SQLLexer.TokenType.valueOf(parts[1]);

            // 修复：处理可能缺失的size参数
            Integer size = null;
            if (parts.length > 2 && !parts[2].isEmpty()) {
                try {
                    size = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    // 忽略无效的size
                }
            }

            table.addColumn(new Column(colName, colType, size));
        }

        for (int i = 1; i < lines.size(); i++) {
            String[] values = lines.get(i).split("\\|");
            if (values.length != table.getColumns().size()) continue;
            Map<String, Object> rowData = new HashMap<>();
            for (int j = 0; j < values.length; j++) {
                Column col = table.getColumns().get(j);
                rowData.put(col.getName().toUpperCase(), parseValue(values[j], col.getType()));
            }
            table.insertRow(new Row(rowData));
        }
        return table;
    }

    private Object parseValue(String value, SQLLexer.TokenType type) {
    if ("NULL".equals(value)) return null;
    try {
        return switch (type) {
            case INT -> Integer.parseInt(value);
            case FLOAT -> Double.parseDouble(value);
            case CHAR, VARCHAR -> value.toUpperCase();
            default -> throw new RuntimeException("不支持的数据类型: " + type);
        };
    } catch (NumberFormatException e) {
        throw new RuntimeException("值 '" + value + "' 无法转换为类型 " + type);
    }
}

    public void execute(SQLParser.SQLStatement statement) {
        try {
            switch (statement.getType()) {
                case CREATE_DATABASE:
                    executeCreateDatabase((SQLParser.CreateDatabaseStatement) statement);
                    break;
                case USE_DATABASE:
                    executeUseDatabase((SQLParser.UseDatabaseStatement) statement);
                    break;
                case CREATE_TABLE:
                    executeCreateTable((SQLParser.CreateTableStatement) statement);
                    break;
                case SHOW_DATABASES:
                    executeShowDatabases();
                    break;
                case SHOW_TABLES:
                    executeShowTables();
                    break;
                case INSERT:
                    executeInsert((SQLParser.InsertStatement) statement);
                    break;
                case SELECT:
                    executeSelect((SQLParser.SelectStatement) statement);
                    break;
                case UPDATE:
                    executeUpdate((SQLParser.UpdateStatement) statement);
                    break;
                case DELETE:
                    executeDelete((SQLParser.DeleteStatement) statement);
                    break;
                case DROP_TABLE:
                    executeDropTable((SQLParser.DropTableStatement) statement);
                    break;
                case DROP_DATABASE:
                    executeDropDatabase((SQLParser.DropDatabaseStatement) statement);
                    break;
                default:
                    throw new RuntimeException("不支持的SQL语句类型");
            }
        } catch (Exception e) {
            throw new RuntimeException("执行错误: " + e.getMessage());
        }
    }

    private void executeCreateDatabase(SQLParser.CreateDatabaseStatement stmt) {
        String dbName = stmt.getDbName();
        if (databases.containsKey(dbName)) {
            throw new RuntimeException("数据库已存在: " + dbName);
        }

        try {
            Files.createDirectory(Paths.get(baseDir, dbName));
            databases.put(dbName, new Database(dbName));
            System.out.println("数据库创建成功: " + dbName);
        } catch (IOException e) {
            throw new RuntimeException("创建失败: " + e.getMessage());
        }
    }

    private void executeUseDatabase(SQLParser.UseDatabaseStatement stmt) {
        String dbName = stmt.getDbName();
        if (!databases.containsKey(dbName)) {
            throw new RuntimeException("数据库不存在: " + dbName);
        }
        currentDatabase = dbName;
        System.out.println("切换到数据库: " + dbName);
    }

    private void executeCreateTable(SQLParser.CreateTableStatement stmt) {
        checkCurrentDatabase();
        Database db = databases.get(currentDatabase);

        if (db.hasTable(stmt.getTableName())) {
            throw new RuntimeException("表已存在: " + stmt.getTableName());
        }

        Table table = new Table(stmt.getTableName());
        stmt.getColumns().forEach(colDef ->
                table.addColumn(convertToColumn(colDef))
        );

        db.addTable(table);
        saveTable(db.getName(), table);
        System.out.println("表创建成功: " + stmt.getTableName());
    }

    private Column convertToColumn(SQLParser.ColumnDefinition parserCol) {
    return new Column(
        parserCol.getName(),
        parserCol.getType(),
        parserCol.getSize()
    );
}

    private void executeShowDatabases() {
        List<String> dbNames = new ArrayList<>(databases.keySet());
        printResult(Collections.singletonList("Database"),
                dbNames.stream().map(Collections::singletonList).collect(Collectors.toList()));
    }

    private void executeShowTables() {
        checkCurrentDatabase();
        // 添加额外的验证逻辑
        if (currentDatabase == null || !databases.containsKey(currentDatabase)) {
            throw new RuntimeException("无法显示表列表: 当前数据库未设置或不存在");
        }
        Database db = databases.get(currentDatabase);

        List<String> tableNames = new ArrayList<>(db.getTables().keySet());
        printResult(Collections.singletonList("Tables_in_" + currentDatabase),
                tableNames.stream().map(Collections::singletonList).collect(Collectors.toList()));
    }

    private void executeInsert(SQLParser.InsertStatement stmt) {
        checkCurrentDatabase();
        Database db = databases.get(currentDatabase);
        Table table = db.getTable(stmt.getTableName());

        if (table == null) {
            throw new RuntimeException("表不存在: " + stmt.getTableName());
        }

        Map<String, Object> rowData = new HashMap<>();
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            Object value = evaluateExpression(stmt.getValues().get(i));
            rowData.put(col.getName(), value);
        }

        table.insertRow(new Row(rowData));
        saveTable(db.getName(), table);
        System.out.println("插入成功，影响1行");
    }

    private void executeSelect(SQLParser.SelectStatement stmt) {
        checkCurrentDatabase();
        Database db = databases.get(currentDatabase);
        //判断是否是多表查询
        List<Row> result;
        if (stmt.getTables().size() > 1) {
            result = handleMultiTableQuery(db, stmt);//进行多表处理
        } else {
            Table table = db.getTable(stmt.getTables().getFirst().getName());
            if (table == null) throw new RuntimeException("表不存在: " + stmt.getTables().getFirst());

            result = table.getRows().stream()
                    .filter(row -> stmt.getWhere() == null || evaluateCondition(stmt.getWhere(), row))
                    .collect(Collectors.toList());
        }

        List<String> columnsToShow = resolveColumnsToShow(stmt, db);
        printQueryResult(result, columnsToShow);
    }

    private List<Row> handleMultiTableQuery(Database db, SQLParser.SelectStatement stmt) {
        // 获取所有参与查询的表信息
        List<Table> tables = stmt.getTables().stream()
                .map(tableRef -> db.getTable(tableRef.getName()))
                .filter(Objects::nonNull)
                .toList();
        // 检查表是否存在
        if (tables.size() != stmt.getTables().size()) {
            throw new RuntimeException("部分表不存在");
        }
        // 计算表的笛卡尔积
        List<Row> cartesianProduct = tables.getFirst().getRows();
        for (int i = 1; i < tables.size(); i++) {
            cartesianProduct = combineTables(cartesianProduct, tables.get(i).getRows());
        }
        // 应用where条件过滤
        return cartesianProduct.stream()
                .filter(row -> stmt.getWhere() == null || evaluateCondition(stmt.getWhere(), row))
                .collect(Collectors.toList());
    }
    // 表连接实现
    private List<Row> combineTables(List<Row> rows1, List<Row> rows2) {
        List<Row> combined = new ArrayList<>();
        for (Row row1 : rows1) {
            for (Row row2 : rows2) {
                //合并两行的数据
                Map<String, Object> newValues = new HashMap<>();
                newValues.putAll(row1.getValues());//添加表1数据
                newValues.putAll(row2.getValues());//添加表2数据
                combined.add(new Row(newValues));
            }
        }
        return combined;
    }

    private List<String> resolveColumnsToShow(SQLParser.SelectStatement stmt, Database db) {
        List<String> columnsToShow = new ArrayList<>();
        if (stmt.getColumns().getFirst() instanceof SQLParser.AllColumnsExpression) {
            // 获取表名列表
            List<String> tableNames = stmt.getTables().stream()
                .map(SQLParser.TableReference::getName) // 假设TableReference有getName()方法
                .collect(Collectors.toList());
            System.out.println("数据源: " + String.join(", ", tableNames));

            for (SQLParser.TableReference tableRef : stmt.getTables()) {
                Table table = db.getTable(tableRef.getName()); // 使用getName()获取表名
                columnsToShow.addAll(table.getColumns().stream()
                    .map(Column::getName)
                    .toList());
            }
        } else {
            for (SQLParser.Expression expr : stmt.getColumns()) {
                if (expr instanceof SQLParser.ColumnReference) {
                    columnsToShow.add(((SQLParser.ColumnReference) expr).getColumnName());
                } else {
                    throw new RuntimeException("不支持的表达式类型: " + expr.getClass().getSimpleName());
                }
            }
        }
        return columnsToShow;
    }

    private void printQueryResult(List<Row> rows, List<String> columns) {
        List<List<String>> formattedRows = new ArrayList<>();
        for (Row row : rows) {
            List<String> formattedRow = new ArrayList<>();
            for (String col : columns) {
                formattedRow.add(row.getValues().get(col).toString());
            }
            formattedRows.add(formattedRow);
        }
        printResult(columns, formattedRows);
    }

    private void printResult(List<String> headers, List<List<String>> rows) {
        System.out.println(String.join(" | ", headers));
        System.out.println("--------------------------------");
        for (List<String> row : rows) {
            System.out.println(String.join(" | ", row));
        }
    }

   private void executeUpdate(SQLParser.UpdateStatement stmt) {
    try {
        // 1. 验证数据库和表
        checkCurrentDatabase();
        Database db = databases.get(currentDatabase);
        if (db == null) throw new RuntimeException("数据库不存在: " + currentDatabase);

        String tableName = stmt.getTableName().toUpperCase();
        Table table = db.getTable(tableName);
        if (table == null) throw new RuntimeException("表不存在: " + tableName);

        // 2. 创建数据快照用于回滚
        List<Row> snapshot = new ArrayList<>();
        for (Row row : table.getRows()) {
            Map<String, Object> rowCopy = new HashMap<>(row.getValues());
            snapshot.add(new Row(rowCopy));
        }

        int affectedRows = 0;
        boolean updateFailed = false;

        // 3. 处理每一行
        for (int i = 0; i < table.getRows().size(); i++) {
            Row row = table.getRows().get(i);
            try {
                // 3.1 检查WHERE条件
                boolean matches = true;
                if (stmt.getWhere() != null) {
                    matches = evaluateCondition(stmt.getWhere(), row);
                }

                if (matches) {
                    affectedRows++;

                    // 3.2 处理每个赋值
                    for (SQLParser.Assignment assign : stmt.getAssignments()) {
                        String columnName = assign.getColumn().toUpperCase();

                        // 查找目标列
                        Column targetCol = null;
                        for (Column col : table.getColumns()) {
                            if (col.getName().equals(columnName)) {
                                targetCol = col;
                                break;
                            }
                        }
                        if (targetCol == null) {
                            throw new RuntimeException("列不存在: " + columnName);
                        }

                        // 计算新值
                        Object newValue = evaluateExpression(assign.getValue(), row, targetCol);

                        // 直接更新行数据
                        row.updateValue(columnName, newValue);
                    }
                }
            } catch (Exception e) {
                updateFailed = true;
                System.err.println("更新行 #" + (i+1) + " 失败: " + e.getMessage());
                break;
            }
        }

        // 4. 处理结果
        if (updateFailed) {
            // 回滚所有更改
            table.getRows().clear();
            table.getRows().addAll(snapshot);
            throw new RuntimeException("更新失败，已回滚");
        }
        else if (affectedRows > 0) {
            // 5. 保存到文件
            saveTable(db.getName(), table);
            System.out.println("更新成功，影响" + affectedRows + "行");

            // 6. 验证文件更新
            verifyTableUpdate(db.getName(), table);
        } else {
            System.out.println("无匹配记录被更新");
        }

    } catch (Exception e) {
        throw new RuntimeException("执行UPDATE失败: " + e.getMessage());
    }
}

    private void executeDelete(SQLParser.DeleteStatement stmt) {
        checkCurrentDatabase();
        Database db = databases.get(currentDatabase);
        Table table = db.getTable(stmt.getTableName());

        if (table == null) {
            throw new RuntimeException("表不存在: " + stmt.getTableName());
        }

        // 1. 创建数据快照用于回滚
        List<Row> originalRows = new ArrayList<>(table.getRows());
        Path tablePath = Paths.get(baseDir, currentDatabase, table.getName() + ".tbl");

        try {
            // 2. 执行内存数据删除
            int affectedRows = deleteRowsFromMemory(table, stmt.getWhere());

            if (affectedRows > 0) {
                // 3. 同步持久化到文件
                persistTableToFile(tablePath, table);

                // 4. 验证文件更新
                validateFileUpdate(tablePath, table);

                System.out.println("删除成功，影响" + affectedRows + "行");
            }
        } catch (Exception e) {
            // 5. 异常时回滚内存数据
            rollbackMemoryData(table, originalRows);
            throw new RuntimeException("删除失败，已回滚: " + e.getMessage());
        }
    }
    //辅助方法
    // 内存数据删除
    private int deleteRowsFromMemory(Table table, SQLParser.Expression whereCondition) {
        int affectedRows = 0;
        Iterator<Row> iterator = table.rows.iterator();
        //很关键，之前用的是getRows()方法返回的只是一个副本所以删除不起作用

        while (iterator.hasNext()) {
            Row row = iterator.next();
            if (whereCondition == null || evaluateCondition(whereCondition, row)) {
                iterator.remove();
                affectedRows++;
            }
        }
        return affectedRows;
    }

    // 文件持久化（同步写入）
    private void persistTableToFile(Path tablePath, Table table) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(tablePath,
                StandardCharsets.UTF_8,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE)) {

            writer.write(serializeTable(table));
            writer.flush(); // 强制立即写入
        }
    }

    // 数据验证
    private void validateFileUpdate(Path tablePath, Table table) throws IOException {
        List<String> lines = Files.readAllLines(tablePath);
        int expectedLines = table.getRows().size() + 1; // 数据行+表头

        if (lines.size() != expectedLines) {
            throw new IOException("文件同步失败: 预期行数=" + expectedLines + ", 实际=" + lines.size());
        }
    }

    // 回滚机制
    private void rollbackMemoryData(Table table, List<Row> originalRows) {
        table.getRows().clear();
        table.getRows().addAll(originalRows);
    }
    private void executeDropTable(SQLParser.DropTableStatement stmt) {
        checkCurrentDatabase();
        Database db = databases.get(currentDatabase);
        if (!db.hasTable(stmt.getTableName())) {
            throw new RuntimeException("表不存在: " + stmt.getTableName());
        }

        db.removeTable(stmt.getTableName());
        Path tablePath = Paths.get(baseDir, currentDatabase, stmt.getTableName() + ".tbl");
        try {
            Files.deleteIfExists(tablePath);
            System.out.println("表删除成功: " + stmt.getTableName());
        } catch (IOException e) {
            throw new RuntimeException("删除表文件失败: " + e.getMessage());
        }
    }

    private void executeDropDatabase(SQLParser.DropDatabaseStatement stmt) {
        String dbName = stmt.getDbName();
        if (!databases.containsKey(dbName)) {
            throw new RuntimeException("数据库不存在: " + dbName);
        }

        if (currentDatabase != null && currentDatabase.equals(dbName)) {
            currentDatabase = null;
        }

        databases.remove(dbName);
        Path dbPath = Paths.get(baseDir, dbName);
        try {
            Files.walk(dbPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            System.out.println("数据库删除成功: " + dbName);
        } catch (IOException e) {
            throw new RuntimeException("删除数据库目录失败: " + e.getMessage());
        }
    }

    private boolean evaluateCondition(SQLParser.Expression condition, Row row) {
        if(condition == null)return true;
        //添加对NULL值的处理
        if (condition instanceof SQLParser.IsNullExpression isNullExpr) {
            Object value = evaluateExpression(isNullExpr.getOperand(), row);
            return (value == null) != isNullExpr.isNot(); // IS NULL或IS NOT NULL
        }
        //补全between表达式
        if (condition instanceof SQLParser.BetweenExpression between) {
            Object value = evaluateExpression(between.getValue(), row);
            Object start = evaluateExpression(between.getStart(), row);
            Object end = evaluateExpression(between.getEnd(), row);
            return compareNumbers(value, start) >= 0 && compareNumbers(value, end) <= 0;
        }
        if (condition instanceof SQLParser.ComparisonExpression comp) {
            Object left = evaluateExpression(comp.getLeft(), row);
            Object right = evaluateExpression(comp.getRight(), row);

            switch (comp.getOperator()) {
                case EQ:
                    // NULL值处理：两个操作数均为NULL时返回true，单一NULL返回false
                    if (left == null && right == null) return true;
                    if (left == null || right == null) return false;
                     // 字符串比较不区分大小写
                    if (left instanceof String && right instanceof String) {
                        return ((String) left).equalsIgnoreCase((String) right);
                    }
                    // 类型转换：数字与字符串比较时统一为Double
                    if (left instanceof Number && right instanceof String) {
                        try {
                            right = Double.parseDouble((String) right);
                        } catch (NumberFormatException e) {
                            return false; // 转换失败视为不相等
                        }
                    } else if (left instanceof String && right instanceof Number) {
                        try {
                            left = Double.parseDouble((String) left);
                        } catch (NumberFormatException e) {
                            return false;
                        }
                    }
                    return left.equals(right);

                case NEQ:
                    // NULL值处理：仅当两个操作数均为NULL时返回false
                    if (left == null && right == null) return false;
                    if (left == null || right == null) return true;
                    // 字符串比较不区分大小写
                    if (left instanceof String && right instanceof String) {
                        return ((String) left).equalsIgnoreCase((String) right);
                    }
                    // 类型转换逻辑与EQ相同
                    if (left instanceof Number && right instanceof String) {
                        try {
                            right = Double.parseDouble((String) right);
                        } catch (NumberFormatException e) {
                            return true; // 转换失败视为不相等
                        }
                    } else if (left instanceof String && right instanceof Number) {
                        try {
                            left = Double.parseDouble((String) left);
                        } catch (NumberFormatException e) {
                            return true;
                        }
                    }
                    return !left.equals(right);

                case GT:
                case GTE:
                case LT:
                case LTE:
                    // NULL值处理：任一操作数为NULL直接返回false
                    if (left == null || right == null) return false;
                    // 数字比较：支持Number子类（Integer/Double等）和可解析的字符串
                    try {
                        double leftNum = (left instanceof Number) ?
                            ((Number) left).doubleValue() : Double.parseDouble(left.toString());
                        double rightNum = (right instanceof Number) ?
                            ((Number) right).doubleValue() : Double.parseDouble(right.toString());
                        switch (comp.getOperator()) {
                            case GT:  return leftNum > rightNum;
                            case GTE: return leftNum >= rightNum;
                            case LT:  return leftNum < rightNum;
                            case LTE: return leftNum <= rightNum;
                        }
                    } catch (NumberFormatException e) {
                        return false; // 非数字类型比较失败
                    }

                default:
                    throw new RuntimeException("不支持的比较运算符: " + comp.getOperator());
            }
        } else if (condition instanceof SQLParser.LogicalExpression logic) {
            boolean left = evaluateCondition(logic.getLeft(), row);
            boolean right = evaluateCondition(logic.getRight(), row);

            return switch (logic.getOperator()) {
                case AND -> {
                    if (!left) yield false;
                    yield right; // 短路优化
                }
                case OR -> {
                    if (left) yield true;
                    yield right; // 短路优化
                }
                default -> throw new RuntimeException("不支持的逻辑运算符");
            };
        }
        throw new RuntimeException("不支持的表达式类型");
    }
    //用于简单常量表达式
    private Object evaluateExpression(SQLParser.Expression expr) {
        return evaluateExpression(expr, null,null);
    }
    //用于WHERE条件
    private Object evaluateExpression(SQLParser.Expression expr,Row row) {
        return evaluateExpression(expr,row,null);
    }
    //用于UPDATE/INSERT赋值
   private Object evaluateExpression(SQLParser.Expression expr, Row row, Column targetCol) {
    try {
        if (expr == null) {
            throw new RuntimeException("表达式不能为null");
        }

        Object result;

        switch (expr) {
            case SQLParser.ColumnReference columnReference -> {
                // 列引用
                String colName = columnReference.getColumnName().toUpperCase();
                if (row == null) {
                    throw new RuntimeException("行数据为null，无法获取列值");
                }

                result = row.getValues().get(colName);

                if (result == null) {
                    System.out.println("警告: 列'" + colName + "'值为NULL");
                }
            }
            case SQLParser.ConstantExpression constantExpression -> {
                // 常量
                result = constantExpression.getValue();
                // 如果是字符串值，转为大写
                if (result instanceof String) {
                    return ((String) result).toUpperCase();
                }
                return result;
            }
            case SQLParser.BinaryExpression binaryExpression ->
                // 二元表达式
                    result = evaluateBinaryExpression(binaryExpression, row);
            default -> throw new RuntimeException("不支持的表达式类型: " + expr.getClass().getSimpleName());
        }

        // 类型转换
        if (targetCol != null) {
            return convertValueToType(result, targetCol.getType());
        }
        return result;

    } catch (Exception e) {
        throw new RuntimeException("表达式求值错误: " + expr + " - " + e.getMessage(), e);
    }
}

// 增强的类型转换方法
private Object convertValueToType(Object value, SQLLexer.TokenType targetType) {
    if (value == null) return null;

    try {
        return switch (targetType) {
            case INT -> {
                if (value instanceof Number) yield ((Number) value).intValue();
                yield Integer.parseInt(value.toString());
            }
            case FLOAT -> {
                if (value instanceof Number) yield ((Number) value).doubleValue();
                yield Double.parseDouble(value.toString());
            }
            case CHAR, VARCHAR -> value.toString();
            default -> value;
        };
    } catch (Exception e) {
        throw new RuntimeException("无法将值 '" + value + "' 转换为 " + targetType);
    }
}

    //处理二元表达式(+,-,*,/)
    private Object evaluateBinaryExpression(SQLParser.BinaryExpression expr, Row row) {
    Object left = evaluateExpression(expr.getLeft(), row, null);  // 中间结果不转换
    Object right = evaluateExpression(expr.getRight(), row, null);

    // 数字类型自动转换
    if (left instanceof Number && right instanceof Number) {
        double leftNum = ((Number) left).doubleValue();
        double rightNum = ((Number) right).doubleValue();

        switch (expr.getOperator()) {
            case PLUS: return leftNum + rightNum;
            case MINUS: return leftNum - rightNum;
            case MULTIPLY: return leftNum * rightNum;
            case DIVIDE:
                if (rightNum == 0) throw new ArithmeticException("除数不能为零");
                return leftNum / rightNum;
        }
    }

    // 字符串连接支持
    if (expr.getOperator() == SQLLexer.TokenType.PLUS) {
        return left.toString() + right.toString();
    }

    throw new RuntimeException("不支持的运算类型: "
        + left.getClass().getSimpleName() + " "
        + expr.getOperator() + " "
        + right.getClass().getSimpleName());
}
    // 辅助方法：转换为Double（用于算术运算）
    private Double convertToDouble(Object value) {
        if (value == null) return null;

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
    // 增强类型检查（支持字符串数字）
    private int compareNumbers(Object left, Object right) {
        Double leftNum = convertToDouble(left);
        Double rightNum = convertToDouble(right);

        if (leftNum == null || rightNum == null) {
            throw new RuntimeException("比较操作只能用于数字类型: " + left + " vs " + right);
        }

        return Double.compare(leftNum, rightNum);
    }

    private void checkCurrentDatabase() {
        if (currentDatabase == null) throw new RuntimeException("请先选择数据库");
        if (!databases.containsKey(currentDatabase)) throw new RuntimeException("当前数据库不存在");
    }

    // 彻底重写的文件保存方法
    // 1. 统一文件保存方法
    private void saveTable(String dbName, Table table) {
        try {
            Path tablePath = Paths.get(baseDir, dbName, table.getName() + ".tbl");
            Files.createDirectories(tablePath.getParent());

            // 使用统一的序列化方法
            String content = serializeTable(table);

            // 写入文件（覆盖模式）
            Files.writeString(tablePath, content,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);

            // 添加验证
            verifyTableUpdate(dbName, table);

        } catch (IOException e) {
            throw new RuntimeException("保存表失败: " + e.getMessage());
        }
    }
    private String serializeTable(Table table) {
        StringBuilder sb = new StringBuilder();

        // 1. 序列化列定义（使用列对象中的实际名称）
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            sb.append(col.getName().toUpperCase())  // 确保列名大写
              .append(":")
              .append(col.getType());
            if (col.getSize() != null) {
                sb.append(":").append(col.getSize());
            }
            if (i < table.getColumns().size() - 1) {
                sb.append("|");
            }
        }
        sb.append("\n");

        // 2. 序列化行数据（使用大写的列名）
        for (Row row : table.getRows()) {
            for (int i = 0; i < table.getColumns().size(); i++) {
                Column col = table.getColumns().get(i);
                String columnName = col.getName().toUpperCase();  // 关键修复
                Object value = row.getValues().get(columnName);

                if (value == null) {
                    sb.append("NULL");
                } else {
                    // 根据类型格式化值
                    switch (col.getType()) {
                        case INT:
                            sb.append(value);
                            break;
                        case FLOAT:
                            sb.append(String.format(Locale.US, "%.2f", value));
                            break;
                        case CHAR:
                        case VARCHAR:
                            sb.append(value);
                            break;
                        default:
                            sb.append(value);
                    }
                }

                if (i < table.getColumns().size() - 1) {
                    sb.append("|");
                }
            }
            sb.append("\n");
        }

        return sb.toString();
    }
    public static class Database {
        private final String name;
        private final Map<String, Table> tables = new HashMap<>();

        public Database(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void addTable(Table table) {
            tables.put(table.getName(), table);
        }

        public void removeTable(String name) {
            tables.remove(name);
        }

        public Table getTable(String name) {
            return tables.get(name);
        }

        public boolean hasTable(String name) {
            return tables.containsKey(name);
        }

        public Map<String, Table> getTables() {
            return Collections.unmodifiableMap(tables);
        }
    }
    public static class Table {
        private final String name;
        private final List<Column> columns = new ArrayList<>();
        public List<Row> rows = new ArrayList<>();

        public Table(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void addColumn(Column column) {
            columns.add(column);
        }

        public List<Column> getColumns() {
            return Collections.unmodifiableList(columns);
        }

        public void insertRow(Row row) {
            rows.add(row);
        }

        public List<Row> getRows() {
            return rows;
        }



    }

    public static class Column {
        private final String name;
        private final SQLLexer.TokenType type;
        private final Integer size;

        public Column(String name, SQLLexer.TokenType type, Integer size) {
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
    // 文件更新验证
    private void verifyTableUpdate(String dbName, Table table) {
        try {
            Path tablePath = Paths.get(baseDir, dbName, table.getName() + ".tbl");

            // 验证文件存在
            if (!Files.exists(tablePath)) {
                throw new RuntimeException("文件保存失败: 文件未创建");
            }

            // 验证文件大小
            long fileSize = Files.size(tablePath);
            if (fileSize == 0) {
                throw new RuntimeException("文件保存失败: 文件大小为0");
            }

            // 重新加载验证数据
            Table reloaded = loadTable(table.getName(), tablePath.toFile());
            if (reloaded.getRows().size() != table.getRows().size()) {
                throw new RuntimeException("文件保存失败: 行数不一致");
            }

        } catch (IOException e) {
            throw new RuntimeException("文件验证失败: " + e.getMessage());
        }
    }
    // 2. 修复行类，使其可修改
    public static class Row {
        private final Map<String, Object> values;

        public Row(Map<String, Object> values) {
            this.values = new HashMap<>(values);
        }

        // 改为可修改的Map
        public Map<String, Object> getValues() {
            return values; // 不再使用unmodifiableMap
        }

        // 添加更新方法
        public void updateValue(String column, Object value) {
            values.put(column, value);
        }
    }

}