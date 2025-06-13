package Formatter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultFormatter {
    public static String formatSelectResult(List<Map<String, Object>> result, 
                                          List<String> columns) {
        if (result.isEmpty()) {
            return "Empty set (0.00 sec)";
        }

        // 计算每列最大宽度
        Map<String, Integer> columnWidths = new HashMap<>();
        for (String col : columns) {
            int maxWidth = col.length();
            for (Map<String, Object> row : result) {
                Object value = row.get(col);
                String strValue = value != null ? value.toString() : "NULL";
                maxWidth = Math.max(maxWidth, strValue.length());
            }
            columnWidths.put(col, maxWidth);
        }

        // 构建表格边框
        StringBuilder sb = new StringBuilder();
        appendDivider(sb, columnWidths, columns);
        
        // 表头
        sb.append("|");
        for (String col : columns) {
            sb.append(" ").append(padRight(col, columnWidths.get(col))).append(" |");
        }
        sb.append("\n");
        appendDivider(sb, columnWidths, columns);

        // 数据行
        for (Map<String, Object> row : result) {
            sb.append("|");
            for (String col : columns) {
                Object value = row.get(col);
                String strValue = value != null ? value.toString() : "NULL";
                sb.append(" ").append(padRight(strValue, columnWidths.get(col))).append(" |");
            }
            sb.append("\n");
        }

        // 表尾和统计信息
        appendDivider(sb, columnWidths, columns);
        sb.append(String.format("%d rows in set (%.2f sec)", 
                              result.size(), 0.00));

        return sb.toString();
    }

    private static void appendDivider(StringBuilder sb, 
                                    Map<String, Integer> widths, 
                                    List<String> columns) {
        sb.append("+");
        for (String col : columns) {
            sb.append("-".repeat(widths.get(col) + 2)).append("+");
        }
        sb.append("\n");
    }

    private static String padRight(String s, int length) {
        return String.format("%-" + length + "s", s);
    }
}