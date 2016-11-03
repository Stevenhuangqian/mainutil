package com.efun.mainland.util.redis.v2;

import com.efun.mainland.util.CommonUtil;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by Efun on 2016-10-31.
 */
public class RedisHelperUtil {

    public static final String NX = "NX";
    public static final String XX = "XX";
    public static final String PX = "PX";
    public static final String EX = "EX";

    public static final <T> byte[] loadKey(Class<T> cls, String key, String cachePrefix) throws Exception {
        return CommonUtil.stringToByte(loadStringKey(cls, key, cachePrefix));
    }

    public static final <T> String loadStringKey(Class<T> cls, String key, String cachePrefix) throws Exception {
        StringBuilder strBuilder = new StringBuilder(80);
        strBuilder.append(cachePrefix);
        if (cls != null) {
            strBuilder.append("-").append(cls.getName());
        }
        strBuilder.append("-").append(key);
        return strBuilder.toString();
    }

    public static final LinkedHashSet<String> string2StringSet(String strs){
        LinkedHashSet<String> sets=new LinkedHashSet<>();
        if (strs!=null)
        for (String temp:strs.split("\\,|\\|")){
            sets.add(temp);
        }
        return sets;
    }

    public static final Set<HostAndPort> string2HostAndPortSet(String strs){
        Set<HostAndPort> sets=new HashSet<>();
        if (strs!=null)
            for (String temp:strs.split("\\,|\\|")){
                String[] hostAndPort=temp.split("\\:");
                if (hostAndPort.length==2)
                sets.add(new HostAndPort(hostAndPort[0],Integer.parseInt(hostAndPort[1])));
            }
        return sets;
    }

}
