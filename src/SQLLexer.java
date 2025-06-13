import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLLexer {
    private final String input;

    // 定义Token类型（增强版）
    public enum TokenType {
        // 关键字（新增USE/DATABASE等）
        CREATE, DATABASE, USE, TABLE,SHOW,TABLES, DATABASES,
        INSERT, INTO, VALUES, SELECT, FROM, WHERE,
        UPDATE, SET, DELETE, DROP, EXIT,NULL,

        // 数据类型
        INT, CHAR, VARCHAR, FLOAT,

        // 运算符和符号（增强比较运算符）
        EQ(), GT(), LT(), GTE(), LTE(), NEQ(),
        PLUS(),MINUS(),MULTIPLY(),DIVIDE(),
        AND, OR, NOT, BACKTICK(),BETWEEN,LIKE,IN,IS,TRUE,FALSE,
        COMMA(), SEMICOLON(), LPAREN(), RPAREN(), DOT(), STAR(),

        // 标识符和常量
        IDENTIFIER, INTEGER, STRING,
        // 结束标记
        EOF;

        TokenType() {
        }
    }

    // Token类
    public static class Token {
        public final TokenType type;
        public final String value;
        public final int startPos;
        public final int endPos;

        public Token(TokenType type, String value, int startPos, int endPos) {
            this.type = type;
            this.value = value;
            this.startPos = startPos;
            this.endPos = endPos;
        }

        @Override
        public String toString() {
            return String.format("Token(%s, '%s', %d-%d)", type, value, startPos, endPos);
        }
    }

    public SQLLexer(String input) {
        this.input = input.trim();
    }

    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();

        // 预编译正则表达式
        String keywordPattern = "(?i)(SHOW|TABLES|DATABASES|CREATE|DATABASE|USE|TABLE|INSERT|INTO|VALUES|SELECT|FROM|WHERE|BETWEEN|AND|OR|UPDATE|SET|DELETE|DROP|EXIT|NULL)";
        String typePattern = "(?i)(INT|CHAR|VARCHAR|FLOAT)";
        String operatorPattern = "(>=|<=|<>|!=|=|>|<)";
        String symbolPattern = "[,;`.*()]";
        String numberPattern = "\\d+";
        String stringPattern = "'[^']*'";
        String identifierPattern = "(`[^`]+`|[a-zA-Z_][a-zA-Z0-9_]*)"; // 支持反引号标识符


        Pattern pattern = Pattern.compile(
            keywordPattern + "|" + typePattern + "|" + operatorPattern + "|" +
            symbolPattern + "|" + numberPattern + "|" + stringPattern + "|" +
            identifierPattern + "|\\s+",Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(input);

        while (matcher.find()) {
            String group = matcher.group();
            int start = matcher.start();
            int end = matcher.end();

            // 关键字匹配（增强USE处理）
            if (Pattern.matches(keywordPattern, group)) {
                TokenType type = TokenType.valueOf(group.toUpperCase());
                tokens.add(new Token(type, group.toUpperCase(), start, end));
            }
            // 数据类型
            else if (Pattern.matches(typePattern, group)) {
                tokens.add(new Token(TokenType.valueOf(group.toUpperCase()), group, start, end));
            }
            // 运算符
            else if (Pattern.matches(operatorPattern, group)) {
                TokenType type = switch (group) {
                    case "=" -> TokenType.EQ;
                    case ">" -> TokenType.GT;
                    case "<" -> TokenType.LT;
                    case ">=" -> TokenType.GTE;
                    case "<=" -> TokenType.LTE;
                    case "!=", "<>" -> TokenType.NEQ;
                    default -> throw new RuntimeException("未知运算符: " + group);
                };
                tokens.add(new Token(type, group, start, end));
            }
            // 符号
            else if (Pattern.matches(symbolPattern, group)) {
                TokenType type = switch (group) {
                    case "," -> TokenType.COMMA;
                    case ";" -> // 确保分号不会被误认为是标识符的一部分
                            TokenType.SEMICOLON;
                    case "(" -> TokenType.LPAREN;
                    case ")" -> TokenType.RPAREN;
                    case "." -> TokenType.DOT;
                    case "*" -> TokenType.STAR;
                    case "`" -> TokenType.BACKTICK;
                    default -> throw new RuntimeException("未知符号: " + group);
                };
                tokens.add(new Token(type, group, start, end));
            }
            // 数字
            else if (Pattern.matches(numberPattern, group)) {
                tokens.add(new Token(TokenType.INTEGER, group, start, end));
            }
            // 字符串
            else if (Pattern.matches(stringPattern, group)) {
                String value = group.substring(1, group.length() - 1);
                tokens.add(new Token(TokenType.STRING, value.toUpperCase(), start, end));
            }
            // 标识符（含反引号）
            else if (Pattern.matches(identifierPattern, group)) {
                String value = group.startsWith("`") ?
                    group.substring(1, group.length() - 1) : group;
                tokens.add(new Token(TokenType.IDENTIFIER, value.toUpperCase(), start, end));
            }
        }

        tokens.add(new Token(TokenType.EOF, "", input.length(), input.length()));
        return tokens;
    }

    // 辅助方法：快速解析USE语句
    public String parseUseDatabase() {
        if (input.toUpperCase().startsWith("USE ")) {
            String dbName = input.substring(4).trim().replace(";", "");
            if (dbName.startsWith("`") && dbName.endsWith("`")) {
                dbName = dbName.substring(1, dbName.length() - 1);
            }
            return dbName.isEmpty() ? null : dbName;
        }
        return null;
    }

    //测试
    public static void main(String[] args) {
        String sql = "USE XJGL; SELECT * FROM `student` WHERE age >= 20;";
        SQLLexer lexer = new SQLLexer(sql);

        // 快速解析USE语句
        String dbName = lexer.parseUseDatabase();
        System.out.println("切换数据库: " + dbName); // 输出"XJGL"

        // 完整词法分析
        List<SQLLexer.Token> tokens = lexer.tokenize();
        tokens.forEach(System.out::println);
    }

}

