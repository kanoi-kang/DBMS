import java.util.List;
import java.util.Scanner;

public class SimpleDBMS {
    public static void main(String[] args) {
        SQLExecutor executor = new SQLExecutor();
        Scanner scanner = new Scanner(System.in);

        System.out.println("简易DBMS系统 (输入EXIT退出)");

        while (true) {
            System.out.print("SQL> ");
            String sql = scanner.nextLine().trim();

            if (sql.equalsIgnoreCase("EXIT")) {
                break;
            }

            try {
                // 词法分析
                SQLLexer lexer = new SQLLexer(sql);
                List<SQLLexer.Token> tokens = lexer.tokenize();

                // 语法分析
                SQLParser parser = new SQLParser(tokens);
                SQLParser.SQLStatement stmt = parser.parse();

                // 执行
                executor.execute(stmt);
            } catch (Exception e) {
                System.err.println("错误: " + e.getMessage());
            }
        }

        scanner.close();
        System.out.println("系统退出");
    }
}