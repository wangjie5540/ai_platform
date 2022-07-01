package com.digitforce.algorithm;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestString {

    public static void main(String[] args) {
        String a="10-20";
//        String regEx="[^0-9]";
//        Pattern p = Pattern.compile(regEx);
//        Matcher m = p.matcher(a);
//        System.out.println( m.replaceAll("").trim());
        int num1 = Integer.parseInt(a.split("-")[0]);
        int num2 = Integer.parseInt(a.split("-")[1]);
        System.out.println(num1 + ", " + num2);
    }
}
