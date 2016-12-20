package com.efun.mainland.util;

import org.springframework.beans.factory.FactoryBean;

public class PropertiesEncryptFactoryBean implements FactoryBean<Object> {

    private Object property;

    public Object getObject() throws Exception {
        return getProperty();
    }

    public Class<?> getObjectType() {
        if (property != null)
            return property.getClass();
        else
            return Object.class;
    }

    public boolean isSingleton() {
        return false;
    }

    public Object getProperty() {
        return property;
    }

    public void setProperty(Object property) {
        String temp = String.valueOf(property);
        try {
            temp = SecretUtil.decrypt(String.valueOf(property), null);
        } catch (Exception e) {
        }
        if (CommonUtil.objectIsNotNull(temp)) {
            this.property = temp;
        } else {
            this.property = String.valueOf(property);
        }
    }
}
