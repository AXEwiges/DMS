package common.meta;

import java.util.ArrayList;
import java.util.List;

public class TestTools {

    public TestTools() {

    }

    public void RInfo(String log, String... args) {
        System.out.print("[");
        System.out.printf("%-25s", log);
        System.out.print("] ");
        for (String s : args)
            System.out.print(s);
        System.out.println();
    }

    public void RInfo(int type, String log, String... args) {
        class color {
            public final String left;
            public final String right;

            public color(String a, String b) {
                left = a;
                right = b;
            }
        }

        List<color> colors = new ArrayList<>() {{
            add(new color("\033[31;4m", "\033[0m")); //错误
            add(new color("\033[32;4m", "\033[0m")); //正确
            add(new color("\033[33;4m", "\033[0m")); //接收
            add(new color("\033[34;4m", "\033[0m")); //发送
            add(new color("\033[35;4m", "\033[0m")); //打印
            add(new color("\033[36;4m", "\033[0m")); //执行
            add(new color("\033[37;4m", "\033[0m")); //分隔
        }};

        String a = colors.get(type).left;
        String b = colors.get(type).right;

        System.out.print(a + "[" + String.format("%-25s", log) + "]" + b);
        System.out.print(" ");


        for (String s : args)
            System.out.print(s);
        System.out.println();
    }
}
