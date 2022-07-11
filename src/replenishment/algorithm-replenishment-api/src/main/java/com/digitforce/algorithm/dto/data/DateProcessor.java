package com.digitforce.algorithm.dto.data;

import com.digitforce.algorithm.dto.ReplRequest;
import lombok.extern.slf4j.Slf4j;
import sun.java2d.pipe.SpanShapeRenderer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@Slf4j
public class DateProcessor {



    public static String addDate(String date, int days)  {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        if (!dateFormat(date, sdf)) {
            date = convertDate(date);
        }

        Calendar c = Calendar.getInstance();
        Date d;
        try {
           d = sdf.parse(date);
        } catch (Exception e) {
            log.error("转化日期报错：{}" , e);
            d = new Date();
        }
        c.setTime(d);
        c.add(Calendar.DATE, days);
        String output = sdf.format(c.getTime());

        return output;
    }


   public static List<String> getDateList(String beginTime, String endTime) {
       SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
       SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
       Date dBegin = null;
       Date dEnd = null;
       try {
            dBegin = sdf.parse(beginTime);
            dEnd = sdf.parse(endTime);
       } catch (ParseException e) {
            e.printStackTrace();
       }
       List<String> daysStrList = new ArrayList<String>();
       Calendar calBegin = Calendar.getInstance();
       calBegin.setTime(dBegin);
       Calendar calEnd = Calendar.getInstance();
       calEnd.setTime(dEnd);
       while (dEnd.after(calBegin.getTime())) {
           calBegin.add(Calendar.DAY_OF_MONTH, 1);
           String dayStr = sdf2.format(calBegin.getTime());
           daysStrList.add(dayStr);
       }
       return daysStrList;
   }

    /** 计算2个日期之间的间隔天数
     *
     * @param date1
     * @param date2
     * @return
     */
   public static int calDaysBetweenTwoDate(String date1, String date2) {
       SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
       if (!dateFormat(date1, sdf)) {
           date1 = convertDate(date1);
       }
       if (!dateFormat(date2, sdf)) {
           date2 = convertDate(date2);
       }

       int diffDays = 0;
       try {
         Date start = sdf.parse(date1);
         Date end = sdf.parse(date2);
         Long startTime = start.getTime();
         Long endTime = end.getTime();
         diffDays = (int) Math.ceil((endTime - startTime) / (24*60*60*1000));
       } catch (ParseException e) {
           e.printStackTrace();
       }
       return  diffDays;
   }

    /** 获取是星期几
     *
     * @return
     */
   public static int calDayOfWeek(String date){

       SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
       if (!dateFormat(date, sdf)) {
           date = convertDate(date);
       }

       int dayOfWeek = 0;
       try {
           Calendar c = Calendar.getInstance();
           c.setTime(sdf.parse(date));
           // 因为默认是从周天开始的，-1后从周一开始
           dayOfWeek = c.get(Calendar.DAY_OF_WEEK) - 1;
       } catch (ParseException e) {
           e.printStackTrace();
       }
       return dayOfWeek;
   }

    /** 判断日期类型是否为指定格式
     *
     * @param date
     * @return
     */
   public static boolean dateFormat(String date, SimpleDateFormat sdf) {
       try {
           Date dateFormat = sdf.parse(date);
           return date.equals(sdf.format(dateFormat));
       } catch (ParseException e) {
           return false;
       }
   }

    /** 转换日期格式
     *
     * @return
     */
   public static String convertDate(String date) {
       try {
           Date format = new SimpleDateFormat("yyyyMMdd").parse(date);
           String convertDare = new SimpleDateFormat("yyyy-MM-dd").format(format);
           return convertDare;
       } catch (ParseException e) {
           e.printStackTrace();
           return date;
       }

   }

   public static String convertDateToYYYYMMDD(String date) {
       try {
           Date format = new SimpleDateFormat("yyyy-MM-dd").parse(date);
           String convertDare = new SimpleDateFormat("yyyyMMdd").format(format);
           return convertDare;
       } catch (ParseException e) {
           e.printStackTrace();
           return date;
       }
   }

   public static void main(String[] args) {
       int dayOfWeek = calDayOfWeek("20220622");
       System.out.println(dayOfWeek);
   }
}
