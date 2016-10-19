package com.efun.mainland.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.efun.general.GeneralHelper;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonUtil {
    private static long sequence;
    private static String compareTime;
    private static NumberFormat numberFormat;
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyMMddHHmmssSSS");
    private static final String upperLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static final char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
            'F'};

    private static final Pattern IP4_PATTERN = Pattern.compile(
            "\\b((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\b");

    static {
        numberFormat = NumberFormat.getInstance();
        numberFormat.setGroupingUsed(false);
        numberFormat.setMinimumIntegerDigits(5);
        numberFormat.setMaximumIntegerDigits(5);
    }

    public final static String getFixedNumber(int minimum, int maximum, long number) {
        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setGroupingUsed(false);
        numberFormat.setMinimumIntegerDigits(minimum);
        numberFormat.setMaximumIntegerDigits(maximum);
        return numberFormat.format(number);
    }

    /**
     * 生成唯一序列号<br/>
     * 根据当前时间加五位序号，一共20位(由纯数字组成)
     *
     * @return 序列号
     */
    public static final synchronized String getSerializableDigit() {
        String currentTime = simpleDateFormat.format(new Date());
        if (compareTime == null || compareTime.compareTo(currentTime) != 0) {
            compareTime = currentTime;
            sequence = 1;
        } else {
            sequence++;
        }
        return currentTime + numberFormat.format(sequence);
    }

    /**
     * 生成唯一序列号<br/>
     * 一共20位(由纯大写字母组成)
     *
     * @return 序列号
     */
    public static final String getSerializableUpperLetter() {
        String serialDigit = getSerializableDigit();
        char[] digits = serialDigit.toCharArray();
        StringBuilder strBuilder = new StringBuilder(20);
        int offset = new Random().nextInt(17);
        String offSetStr = upperLetters.substring(offset, offset + 10);
        for (char tempChar : digits) {
            strBuilder.append(offSetStr.charAt(Integer.valueOf(tempChar + "")) + "");
        }
        return strBuilder.toString();
    }

    /**
     * 生成唯一序列号<br/>
     * 一共20位(由纯小写字母组成)
     *
     * @return 序列号
     */
    public static final String getSerializableLowerLetter() {
        return getSerializableUpperLetter().toLowerCase();
    }

    /**
     * if(object==null||object.toString().trim().length()==0){ return true;
     * }else{ return false; }
     *
     * @param object
     * @return
     */
    public static final boolean objectIsNull(Object object) {
        return object == null || object.toString().trim().length() == 0;
    }

    /**
     * if (object == null) {return "";} else {return object.toString().trim();}
     *
     * @param object
     * @return
     */
    public static final String objectToString(Object object) {
        if (object == null) {
            return "";
        } else {
            return object.toString().trim();
        }
    }

    /**
     * if(object!=null&&object.toString().trim().length()>0){ return true;
     * }else{ return false; }
     *
     * @param object
     * @return
     */
    public static final boolean objectIsNotNull(Object object) {
        return object != null && object.toString().trim().length() > 0;
    }

    /**
     * if(string==null){ return null; }else{ return string.trim(); }
     *
     * @param string
     * @return
     */
    public static final String stringTrim(String string) {
        if (string == null) {
            return null;
        } else {
            return string.trim();
        }
    }

    /**
     * if(string==null){ return 0; }else{ return string.trim().length(); }
     *
     * @param string
     * @return 字符个数
     */
    public static final int stringTrimLength(String string) {
        if (string == null) {
            return 0;
        } else {
            return string.trim().length();
        }
    }

    /**
     * if (string == null) { return 0; } else { return
     * string.trim().getBytes().length; } UTF-8
     *
     * @param string
     * @return 所占字节个数
     */
    public static final int stringByteSize(String string) {
        if (string == null) {
            return 0;
        } else {
            try {
                return string.trim().getBytes("UTF-8").length;
            } catch (UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                return string.trim().getBytes().length;
            }
        }
    }

    /**
     * if (string == null) { return 0; } else { return
     * string.trim().getBytes().length; }
     *
     * @param string
     * @param charset
     * @return 所占字节个数
     */
    public static final int stringByteSize(String string, String charset) {
        if (string == null) {
            return 0;
        } else {
            try {
                return string.trim().getBytes(charset).length;
            } catch (UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                return 0;
            }
        }
    }

    /**
     * MD5加密
     *
     * @param str 原字符串
     * @return 进行MD5加密后的字符串(32个字节长度)
     */
    @Deprecated
    public static final String MD5(String str) {

        try {
            if (str == null) {
                return null;
            }
            // byte[] strTemp = str.getBytes("UTF-8");
            // // 使用MD5创建MessageDigest对象
            // MessageDigest mdTemp = MessageDigest.getInstance("MD5");
            // mdTemp.update(strTemp);
            // byte[] md = mdTemp.digest();
            // int j = md.length;
            // char strChar[] = new char[j * 2];
            // int k = 0;
            // for (int i = 0; i < j; i++) {
            // byte b = md[i];
            // // System.out.println((int)b);
            // // 将每个数(int)b进行双字节加密
            // strChar[k++] = hexDigits[b >> 4 & 0xf];
            // strChar[k++] = hexDigits[b & 0xf];
            // }
            // return new String(strChar);
            return MD5Util.getMD5String(str);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * SHA加密
     *
     * @param str 原字符串
     * @return 进行SHA加密后的字符串(40个字节长度)
     */
    public static final String SHA(String str) {
        try {
            if (str == null) {
                return null;
            }
            byte[] strTemp = str.getBytes("UTF-8");
            // 使用SHA创建MessageDigest对象
            MessageDigest shaTemp = MessageDigest.getInstance("SHA");
            shaTemp.update(strTemp);
            byte[] sha = shaTemp.digest();
            int j = sha.length;
            char strChar[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte b = sha[i];
                // System.out.println((int)b);
                // 将每个数(int)b进行双字节加密
                strChar[k++] = hexDigits[b >> 4 & 0xf];
                strChar[k++] = hexDigits[b & 0xf];
            }
            return new String(strChar);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 冒泡排序
     *
     * @param strs
     * @return
     */
    public static final String[] bubbleSort(String[] strs) {
        int a[] = new int[strs.length];
        for (int i = 0; i < strs.length; i++) {
            a[i] = strs[i].charAt(0);
        }
        int temp = 0;
        String tempStr = "";
        for (int i = 0; i < a.length - 1; i++) {
            for (int j = 0; j < a.length - 1 - i; j++) {
                if (a[j] > a[j + 1]) {
                    temp = a[j];
                    a[j] = a[j + 1];
                    a[j + 1] = temp;

                    tempStr = strs[j];
                    strs[j] = strs[j + 1];
                    strs[j + 1] = tempStr;
                }
            }
        }
        return strs;
    }

    /**
     * 用空格代替回车换行
     *
     * @param str
     * @return
     */
    public static final String replaceAllLnLr(String str) {
        if (str != null) {
            return str.replaceAll("(\r\n|\r|\n|\n\r)", "");
        } else {
            return "";
        }
    }

    public static final String getFieldName(String methodName) {
        return changeFirstCharacter(methodName.substring(3), false);
    }

    public static final String changeFirstCharacter(String str, boolean isUpper) {
        byte[] items = str.getBytes();
        char tempChar = (char) items[0];
        if (isUpper) {
            if (Character.isLowerCase(tempChar)) {
                items[0] = (byte) ((char) items[0] - 'a' + 'A');
            }
        } else {
            if (Character.isUpperCase(tempChar)) {
                items[0] = (byte) ((char) items[0] + 'a' - 'A');
            }
        }
        return new String(items);
    }

    /**
     * 获取obj中key值为name的value值，找不到时返回空字符串
     *
     * @param obj
     * @param name
     * @return
     */
    public static final String getStringValue(Map<String, Object> obj, String name) {
        try {
            Object temp = obj.get(name);
            return temp == null ? "" : temp.toString();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            return "";
        }
    }

    /**
     * 获取obj中key值为name的value值，找不到时返回null
     *
     * @param obj
     * @param name
     * @return
     */
    public static final String getStringValueAllowNull(Map<String, Object> obj, String name) {
        try {
            Object temp = obj.get(name);
            return temp == null ? null : temp.toString();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 将obj转换为jsonobject
     *
     * @param valueAllowEmpty
     * @param obj
     * @return
     */
    public static final JSONObject getJsonFromObject(Object obj, boolean valueAllowEmpty) {
        JSONObject jsonObj = new JSONObject();
        if (obj == null) {
            return jsonObj;
        }
        Method[] sourceMethods = obj.getClass().getMethods();
        try {
            for (int i = 0, len = sourceMethods.length; i < len; i++) {
                if (sourceMethods[i].getName().startsWith("get")) {
                    String lsName = getFieldName(sourceMethods[i].getName()); // 属性
                    String lsSourceType = sourceMethods[i].getReturnType().getName(); // 类型
                    Object loValue = sourceMethods[i].invoke(obj); // 值
                    // System.out.println(lsName+">>>>>"+loValue);
                    if (CommonUtil.objectIsNotNull(loValue)) {
                        if (lsSourceType.equals("java.util.Date")) {
                            if (loValue != null) {
                                jsonObj.put(lsName, dateTimeToString((Date) loValue));
                            }
                        } else if (lsSourceType.equals("java.lang.Class")) {
                        } else if (lsSourceType.indexOf("java.util.") != -1) {

                        } else {
                            jsonObj.put(lsName, String.valueOf(loValue));
                        }
                    } else if (valueAllowEmpty) {
                        jsonObj.put(lsName, "");
                    }
                }
            }
            return jsonObj;
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将map转换为实体对象
     *
     * @param <T>
     * @param map
     * @param obj
     * @return
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws Exception
     */
    public static final <T> T copyMap2Object(Map<String, Object> map, T obj) throws
            Exception {
        if (map == null) {
            return obj;
        } else {
            // 拿到该类
            Class<?> clz = obj.getClass();
            // 获取实体类的所有属性，返回Field数组
            Field[] fields = clz.getDeclaredFields();

            for (Field field : fields) {// --for() begin
                // System.out.println(field.getGenericType());// 打印该类的所有属性类型
                field.setAccessible(true);
                String tempStr = getStringValueAllowNull(map, field.getName());
                if (tempStr == null) {
                    continue;
                }

                String filedTypeStr = field.getGenericType().toString();

                // 如果类型是String
                if (filedTypeStr.equals("class java.lang.String")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            String.class);
                    m.invoke(obj, tempStr);
                }

                // 如果类型是Date
                else if (filedTypeStr.equals("class java.util.Date")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Date.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        tempStr = tempStr.trim();
                        if (CommonUtil.isNumeric(tempStr)) {
                            m.invoke(obj, new Date(Long.parseLong(tempStr)));
                        } else {
                            m.invoke(obj, GeneralHelper.str2Date(tempStr));
                        }
                    }

                }

                // 如果类型是Integer
                else if (filedTypeStr.equals("class java.lang.Integer")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Integer.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Integer.valueOf(tempStr));
                    }

                }

                // 如果类型是Double
                else if (filedTypeStr.equals("class java.lang.Double")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Double.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Double.valueOf(tempStr));
                    }

                }
                // 如果类型是Float
                else if (filedTypeStr.equals("class java.lang.Float")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Float.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Float.valueOf(tempStr));
                    }

                }

                // 如果类型是Boolean 是封装类
                else if (filedTypeStr.equals("class java.lang.Boolean")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Boolean.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Boolean.valueOf(tempStr));
                    }

                }
                // 如果类型是Short
                else if (filedTypeStr.equals("class java.lang.Short")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Short.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Short.valueOf(tempStr));
                    }

                } // 如果类型是Long
                else if (filedTypeStr.equals("class java.lang.Long")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Long.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Long.valueOf(tempStr));
                    }
                }
            }
        }
        return obj;
    }

    /**
     * 将json转换为实体对象
     *
     * @param <T>
     * @param json
     * @param obj
     * @return
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws Exception
     */
    public static final <T> T getObjectFromJson(JSONObject json, T obj) throws
            Exception {
        if (json == null) {
            return obj;
        } else {
            // 拿到该类
            Class<?> clz = obj.getClass();
            // 获取实体类的所有属性，返回Field数组
            Field[] fields = clz.getDeclaredFields();

            for (Field field : fields) {// --for() begin
                // System.out.println(field.getGenericType());// 打印该类的所有属性类型
                field.setAccessible(true);
                String tempStr = getStringValueAllowNull(json, field.getName());
                if (tempStr == null) {
                    continue;
                }

                String filedTypeStr = field.getGenericType().toString();

                // 如果类型是String
                if (filedTypeStr.equals("class java.lang.String")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            String.class);
                    m.invoke(obj, tempStr);
                }

                // 如果类型是Date
                else if (filedTypeStr.equals("class java.util.Date")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Date.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        tempStr = tempStr.trim();
                        if (CommonUtil.isNumeric(tempStr)) {
                            m.invoke(obj, new Date(Long.parseLong(tempStr)));
                        } else {
                            m.invoke(obj, GeneralHelper.str2Date(tempStr));
                        }
                    }

                }

                // 如果类型是Integer
                else if (filedTypeStr.equals("class java.lang.Integer")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Integer.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Integer.valueOf(tempStr));
                    }

                }

                // 如果类型是Double
                else if (filedTypeStr.equals("class java.lang.Double")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Double.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Double.valueOf(tempStr));
                    }

                }
                // 如果类型是Float
                else if (filedTypeStr.equals("class java.lang.Float")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Float.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Float.valueOf(tempStr));
                    }

                }

                // 如果类型是Boolean 是封装类
                else if (filedTypeStr.equals("class java.lang.Boolean")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Boolean.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Boolean.valueOf(tempStr));
                    }

                }
                // 如果类型是Short
                else if (filedTypeStr.equals("class java.lang.Short")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Short.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Short.valueOf(tempStr));
                    }

                } // 如果类型是Long
                else if (filedTypeStr.equals("class java.lang.Long")) {
                    Method m = obj.getClass().getMethod("set" + changeFirstCharacter(field.getName(), true),
                            Long.class);
                    if (CommonUtil.objectIsNotNull(tempStr)) {
                        m.invoke(obj, Long.valueOf(tempStr));
                    }
                }
            }
        }
        return obj;
    }

    /**
     * 序列化
     *
     * @param o
     * @return
     * @throws Exception
     */
    public static final byte[] serialize(Object o) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(); // 构造一个字节输出流
        ObjectOutputStream oos = new ObjectOutputStream(baos); // 构造一个类输出流
        oos.writeObject(o); // 写这个对象
        byte[] buf = baos.toByteArray(); // 从这个地层字节流中把传输的数组给一个新的数组
        oos.flush();
        oos.close();
        baos.close();
        return buf;
    }

    /**
     * 反序列化
     *
     * @param bytes
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static final Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        // 构建一个类输入流，地层用字节输入流实现
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object obj = ois.readObject();
        ois.close();
        return obj;
    }

    /**
     * 判断是否数字
     *
     * @param str
     * @return
     * @see #isPureNumber(CharSequence)
     */
    public static final boolean isNumeric(String str) {
//        return str != null && str.matches("\\d+");
        return isPureNumber(str);
    }

    /**
     * 是否是纯数字（不含.+-）
     *
     * @param charSequence
     * @return
     */
    public static final boolean isPureNumber(CharSequence charSequence) {
        if (charSequence == null || charSequence.length() == 0) {
            return false;
        } else {
            final int sz = charSequence.length();
            for (int i = 0; i < sz; i++) {
                if (!Character.isDigit(charSequence.charAt(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * 判断是否数字(可以有小数点)
     *
     * @param str
     * @return
     */
    public static final boolean isNumDecimal(String str) {
        if (str != null && str.length() > 0) {
            Pattern pattern = Pattern.compile("^\\d+$|^\\d+\\.\\d+$|-\\d+$");
            Matcher isNum = pattern.matcher(str);
            return isNum.matches();
        } else {
            return false;
        }
    }

    /**
     * 字符串转换为日期时间
     *
     * @param strDateTime
     * @return
     */
    public static final Date stringToDateTime(String strDateTime) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setLenient(false);
        try {
            return df.parse(strDateTime);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 日期时间转换为字符串
     *
     * @param date
     * @return
     */
    public static final String dateTimeToString(Date date) {
        try {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return df.format(date);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 字符串转换为日期时间
     *
     * @param strDateTime
     * @param sdf
     * @return
     */
    public static final Date stringToDateTime(String strDateTime, String sdf) {
        SimpleDateFormat df = new SimpleDateFormat(sdf);
        try {
            return df.parse(strDateTime);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 日期时间转换为字符串
     *
     * @param dateTime
     * @param sdf
     * @return
     */
    public static final String dateTimeToString(Date dateTime, String sdf) {
        SimpleDateFormat df = new SimpleDateFormat(sdf);
        return df.format(dateTime);
    }

    /**
     * 将字符串还原成图片
     *
     * @param str
     * @return
     */
    public static final File stringToFile(String str, String fileFullPath) {
        byte[] imgByte = hex2byte(str);
        try {
            InputStream in = new ByteArrayInputStream(imgByte);
            File file = new File(fileFullPath);
            FileOutputStream fos = new FileOutputStream(file);
            int i = in.read();
            while (i != -1) {
                fos.write(i);
                i = in.read();
            }
            fos.flush();
            fos.close();
            in.close();
            return file;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将文件转换成字符串
     *
     * @param file
     * @return
     */
    public static final String fileToString(File file) {
        // 将文件转换成字符串
        try {
            FileInputStream fis = new FileInputStream(file);
            byte[] bytes = new byte[fis.available()];
            fis.read(bytes);
            fis.close();

            // 生成字符串
            String imgStr = byte2hex(bytes);
            System.out.println(imgStr);
            return imgStr;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 二进制转字符串
     *
     * @param b
     * @return
     */
    public static final String byte2hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        String stmp = "";
        for (int n = 0, len = b.length; n < len; n++) {
            stmp = Integer.toHexString(b[n] & 0XFF);
            if (stmp.length() == 1) {
                sb.append("0");
            }
            sb.append(stmp);

        }
        return sb.toString();
    }

    /**
     * 字符串转二进制
     *
     * @param str
     * @return
     */
    public static final byte[] hex2byte(String str) {
        if (str == null)
            return null;
        str = str.trim();
        int len = str.length();
        if (len <= 0 || len % 2 == 1)
            return null;
        byte[] b = new byte[len / 2];
        try {
            for (int i = 0; i < len; i += 2) {
                b[i / 2] = (byte) Integer.decode("0X" + str.substring(i, i + 2)).intValue();
            }
            return b;
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static final <T> T get(byte[] bytes, Class<T> classType) throws Exception {
        if (bytes == null)
            return null;
        else
            return (T) CommonUtil.deserialize(bytes);
    }

    /**
     * 判断是否为合法ip地址
     *
     * @param ip
     * @return
     */
    public static final boolean isBoolIp(String ip) {
        if (objectIsNull(ip)) {
            return false;
        }
        return IP4_PATTERN.matcher(ip).matches();
    }

    /**
     * 转换为字符串
     *
     * @param objects
     * @return
     */
    public static final <T> String objectConvertString(@SuppressWarnings("unchecked") T... objects) {
        if (objects == null || objects.length < 1) {
            return "null";
        } else {
            StringBuilder strBuilder = new StringBuilder();
            for (Object object : objects) {
                if (object == null) {
                    strBuilder.append(",null");
                } else if (object instanceof String || object instanceof Number || object instanceof Character
                        || object instanceof Boolean) {
                    strBuilder.append(",").append(object);
                } else if (object instanceof Class<?>) {
                    strBuilder.append(",").append(((Class<?>) object).getName());
                } else if (object instanceof Object[]) {
                    strBuilder.append(",(");
                    strBuilder.append(objectConvertString((Object[]) object));
                    strBuilder.append(")");
                } else if (object instanceof byte[]) {
                    strBuilder.append(",");
                    strBuilder.append(Arrays.toString((byte[]) object));
                } else if (object instanceof short[]) {
                    strBuilder.append(",");
                    strBuilder.append(Arrays.toString((short[]) object));
                } else if (object instanceof char[]) {
                    strBuilder.append(",");
                    strBuilder.append(Arrays.toString((char[]) object));
                } else if (object instanceof int[]) {
                    strBuilder.append(",");
                    strBuilder.append(Arrays.toString((int[]) object));
                } else if (object instanceof float[]) {
                    strBuilder.append(",");
                    strBuilder.append(Arrays.toString((float[]) object));
                } else if (object instanceof double[]) {
                    strBuilder.append(",");
                    strBuilder.append(Arrays.toString((double[]) object));
                } else if (object instanceof long[]) {
                    strBuilder.append(",");
                    strBuilder.append(Arrays.toString((long[]) object));
                } else if (object instanceof boolean[]) {
                    strBuilder.append(",");
                    strBuilder.append(Arrays.toString((boolean[]) object));
                } else {
                    strBuilder.append(",").append(JSON.toJSONString(object));
                }
            }
            try {
                return strBuilder.deleteCharAt(0).toString();
            } finally {
                strBuilder.setLength(0);
                strBuilder = null;
            }
        }
    }

    /**
     * 转换字符串,拼接
     *
     * @param strBuilder
     * @param objects
     * @return
     * @see #objectConvertString
     */
    public static final <T> StringBuilder objectConvertString(StringBuilder strBuilder,
                                                              @SuppressWarnings("unchecked") T... objects) {
        if (objects == null || objects.length < 1) {
            return strBuilder;
        } else {
            if (strBuilder == null) {
                strBuilder = new StringBuilder();
            }
            for (Object object : objects) {
                if (object == null) {
                    strBuilder.append(",null");
                } else if (object instanceof String || object instanceof Number || object instanceof Character
                        || object instanceof Boolean) {
                    strBuilder.append(object).append(",");
                } else if (object instanceof Class<?>) {
                    strBuilder.append(",").append(((Class<?>) object).getName());
                } else if (object instanceof Object[]) {
                    strBuilder.append("(");
                    strBuilder.append(objectConvertString((Object[]) object));
                    strBuilder.append("),");
                } else if (object instanceof byte[]) {
                    strBuilder.append(Arrays.toString((byte[]) object));
                    strBuilder.append(",");
                } else if (object instanceof short[]) {
                    strBuilder.append(Arrays.toString((short[]) object));
                    strBuilder.append(",");
                } else if (object instanceof char[]) {
                    strBuilder.append(Arrays.toString((char[]) object));
                    strBuilder.append(",");
                } else if (object instanceof int[]) {
                    strBuilder.append(Arrays.toString((int[]) object));
                    strBuilder.append(",");
                } else if (object instanceof float[]) {
                    strBuilder.append(Arrays.toString((float[]) object));
                    strBuilder.append(",");
                } else if (object instanceof double[]) {
                    strBuilder.append(Arrays.toString((double[]) object));
                    strBuilder.append(",");
                } else if (object instanceof long[]) {
                    strBuilder.append(Arrays.toString((long[]) object));
                    strBuilder.append(",");
                } else if (object instanceof boolean[]) {
                    strBuilder.append(Arrays.toString((boolean[]) object));
                    strBuilder.append(",");
                } else {
                    strBuilder.append(JSON.toJSONString(object)).append(",");
                }
            }
            return strBuilder.deleteCharAt(strBuilder.lastIndexOf(","));
        }
    }

    /**
     * 字符串转换为字节数组(UTF-8)
     *
     * @param str
     * @return
     */
    public static final byte[] stringToByte(String str) throws UnsupportedEncodingException {
        return stringToByte(str, "UTF-8");
    }

    /**
     * 字符串转换为字节数组
     *
     * @param str
     * @param chartset
     * @return
     * @throws UnsupportedEncodingException
     */
    public static final byte[] stringToByte(String str, String chartset) throws UnsupportedEncodingException {
        return str.getBytes(chartset);
    }

    /**
     * 字节数组转换为字符串(UTF-8)
     *
     * @param value
     * @return
     */
    public static final String byteToString(byte[] value) {
        try {
            return byteToString(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            return new String(value);
        }
    }

    /**
     * 字节数组转换为字符串
     *
     * @param value
     * @param chartset
     * @return
     * @throws UnsupportedEncodingException
     */
    public static final String byteToString(byte[] value, String chartset) throws UnsupportedEncodingException {
        try {
            return new String(value, chartset);
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            throw e;
        }
    }
}
