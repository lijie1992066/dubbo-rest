package cn.lijie.comm;

public class CommUtils {

    public static String reverse(String s) {
        if (s == null) {
            s = "";
        }
        char[] array = s.toCharArray();
        String reverse = "";  //注意这是空串，不是null
        for (int i = array.length - 1; i >= 0; i--)
            reverse += array[i];
        return reverse;
    }
}
