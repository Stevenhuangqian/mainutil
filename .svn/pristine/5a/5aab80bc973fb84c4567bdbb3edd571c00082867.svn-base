package com.efun.mainland.util;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.efun.general.BeanHelper;

/**
 * obj[0]为sql语句,obj[1]为条件值（Object[]）
 * 
 * @author Efun
 *
 */
public class SqlUtil {

	private static final SimpleDateFormat getSimpleDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	/**
	 * 获取select语句
	 * <p>
	 * 单表
	 * 
	 * @param obj
	 *            实体对象
	 * @param tableName
	 *            表名（为空时，表名为实体类类名）
	 * @param selectFields
	 *            查询字段（select和from之间的部分，为空时，值为 * ）
	 * @param otherConditions
	 *            其他约束条件
	 * @return obj[0]为sql语句,obj[1]为条件值（Object[]）
	 */
	public static final Object[] getSelectSql(Object obj, String tableName, String selectFields,
			String... otherConditions) {
		Map<String, Object> aliasTableMap = new HashMap<String, Object>(1);
		Map<String, Object> aliasEntityMap = new HashMap<String, Object>(1);
		if (CommonUtil.objectIsNull(tableName)) {
			if (CommonUtil.objectIsNull(obj)) {
				return null;
			} else {
				tableName = obj.getClass().getSimpleName();
			}
		}
		aliasTableMap.put("t", tableName);
		aliasEntityMap.put("t", obj);
		return getSelectSql(aliasTableMap, aliasEntityMap, selectFields, otherConditions);
	}

	/**
	 * /** 获取update语句
	 * <p>
	 * 
	 * @param obj
	 *            实体对象
	 * @param tableName
	 *            表名（为空时，表名为实体类类名）
	 * @param fieldValueMap
	 *            条件（key为字段名，value为字段值）
	 * @return obj[0]为sql语句,obj[1]为条件值（Object[]）
	 */
	public static final Object[] getUpdateSql(Object obj, String tableName, Map<String, Object> fieldValueMap) {
		if (CommonUtil.objectIsNull(obj)) {
			return null;
		} else {
			StringBuilder strBuilder = new StringBuilder();
			strBuilder.append("UPDATE ");
			if (CommonUtil.objectIsNull(tableName)) {
				strBuilder.append(obj.getClass().getSimpleName());
			} else {
				strBuilder.append(tableName);
			}
			strBuilder.append(" SET ");

			StringBuilder sb = new StringBuilder();
			List<Object> list = new ArrayList<Object>();
			PropertyDescriptor proper = null;
			Set<Field> fields = BeanHelper.getAllFields(obj.getClass());
			for (Field field : fields) {
				try {
					proper = new PropertyDescriptor(field.getName(), obj.getClass());
					Method method = proper.getReadMethod();
					if(method!=null){
						Object objOne = method.invoke(obj);
						if (objOne != null) {
							sb.append(",").append(field.getName()).append("=?");
							if (StringUtils.containsIgnoreCase(method.getReturnType() + "", "date")) {
								list.add(getSimpleDateFormat().format((Date) objOne));
							} else {
								list.add(objOne);
							}
						}
					}
				} catch (Exception e) {
					continue;
				}
			}
			strBuilder.append(sb.substring(1));

			if (fieldValueMap != null && fieldValueMap.size() > 0) {
				strBuilder.append(" WHERE ");
				StringBuilder sbapend = new StringBuilder();
				Set<Entry<String, Object>> tempSet = fieldValueMap.entrySet();
				int i = 0;
				for (Entry<String, Object> keyValue : tempSet) {
					if (i == 0) {
						sbapend.append(keyValue.getKey()).append("=?");
					} else {
						sbapend.append(" AND ").append(keyValue.getKey()).append("=?");
					}
					list.add(keyValue.getValue());
					i++;
				}
				strBuilder.append(sbapend.toString());
			}
			return new Object[] { strBuilder.toString(), list.toArray() };
		}
	}

	/**
	 * 获取insert语句
	 * <p>
	 * 
	 * @param obj
	 *            实体对象
	 * @param tableName
	 *            表名（为空时，表名为实体类类名）
	 * @return obj[0]为sql语句,obj[1]为条件值（Object[]）
	 */
	public static final Object[] getInsertSql(Object obj, String tableName) {
		if (CommonUtil.objectIsNull(obj)) {
			return null;
		} else {
			StringBuilder strBuilder = new StringBuilder();
			strBuilder.append("INSERT INTO ");
			if (CommonUtil.objectIsNull(tableName)) {
				strBuilder.append(obj.getClass().getSimpleName());
			} else {
				strBuilder.append(tableName);
			}
			strBuilder.append("(");

			StringBuilder sb = new StringBuilder();
			StringBuilder sbapend = new StringBuilder();
			List<Object> list = new ArrayList<Object>();
			PropertyDescriptor proper = null;
			Set<Field> fields = BeanHelper.getAllFields(obj.getClass());
			for (Field field : fields) {
				try {
					proper = new PropertyDescriptor(field.getName(), obj.getClass());
					Method method = proper.getReadMethod();
					if(method!=null){
						Object objOne = method.invoke(obj);
						if (objOne != null) {
							sb.append(",").append(field.getName());
							sbapend.append(",?");
							if (StringUtils.containsIgnoreCase(method.getReturnType() + "", "date")) {
								list.add(getSimpleDateFormat().format((Date) objOne));
							} else {
								list.add(objOne);
							}
						}
					}
				} catch (Exception e) {
					continue;
				}
			}
			strBuilder.append(sb.substring(1));

			strBuilder.append(") VALUES (");
			strBuilder.append(sbapend.substring(1)).append(")");
			return new Object[] { strBuilder.toString(), list.toArray() };
		}
	}

	/**
	 * 获取select语句
	 * <p>
	 * 多表
	 * 
	 * @param aliasTableMap
	 *            key值为表别名，value值为表名
	 * @param aliasEntityMap
	 *            key值为表别名，value值为实体对象
	 * @param selectFields
	 *            查询字段（select和from之间的部分，为空时，值为所有表的所有字段 ）
	 * @param otherConditions
	 *            其他约束条件
	 * @return obj[0]为sql语句,obj[1]为条件值（Object[]）
	 */
	public static final Object[] getSelectSql(Map<String, Object> aliasTableMap, Map<String, Object> aliasEntityMap,
			String selectFields, String... otherConditions) {
		try {
			ArrayList<Object> valuesList = new ArrayList<Object>();
			StringBuilder strBuilder = new StringBuilder();
			if (aliasTableMap == null || aliasTableMap.size() == 0) {
				if (aliasEntityMap == null || aliasEntityMap.size() == 0) {
					return null;
				} else {
					Set<Entry<String, Object>> tempSet = aliasEntityMap.entrySet();
					int tableSize = aliasEntityMap.size();
					if (CommonUtil.objectIsNull(selectFields)) {
						if (tableSize == 1) {
							strBuilder.append("SELECT * FROM ")
									.append(aliasEntityMap.values().toArray()[0].getClass().getSimpleName());
						} else {
							strBuilder.append("SELECT ");
							int i = 0;
							for (Entry<String, Object> entry : tempSet) {
								if (i == 0) {
									strBuilder.append(entry.getKey()).append(".*");
								} else {
									strBuilder.append(",").append(entry.getKey()).append(".*");
								}
								i++;
							}
							strBuilder.append(" FROM ");
							i = 0;
							for (Entry<String, Object> entry : tempSet) {
								if (i == 0) {
									strBuilder.append(entry.getValue().getClass().getSimpleName()).append(" ")
											.append(entry.getKey());
								} else {
									strBuilder.append(",").append(entry.getValue().getClass().getSimpleName())
											.append(" ").append(entry.getKey());
								}
								i++;
							}
						}
					} else {
						strBuilder.append("SELECT ").append(selectFields).append(" FROM ");
						if (tableSize == 1) {
							strBuilder.append(aliasEntityMap.values().toArray()[0].getClass().getSimpleName());
						} else {
							int i = 0;
							for (Entry<String, Object> entry : tempSet) {
								if (i == 0) {
									strBuilder.append(entry.getValue().getClass().getSimpleName()).append(" ")
											.append(entry.getKey());
								} else {
									strBuilder.append(",").append(entry.getValue().getClass().getSimpleName())
											.append(" ").append(entry.getKey());
								}
								i++;
							}
						}
					}
					strBuilder.append(" WHERE 1=1");
					if (tableSize == 1) {
						Object obj = aliasEntityMap.values().toArray()[0];
						if (obj != null) {
							Set<Method> sourceMethods = BeanHelper.getAllMethods(obj.getClass());
							for (Method method:sourceMethods) {
								if (method.getName().startsWith("get")) {
									if (method.getName().equals("getClass")) {
										continue;
									}
									Object loValue = method.invoke(obj);
									if (CommonUtil.objectIsNotNull(loValue)) {
										strBuilder.append(" AND ")
												.append(CommonUtil.getFieldName(method.getName()))
												.append("=?");
										valuesList.add(loValue);
									}
								}
							}
						}
					} else {
						for (Entry<String, Object> entry : tempSet) {
							Object obj = entry.getValue();
							if (obj != null) {
								Set<Method> sourceMethods = BeanHelper.getAllMethods(obj.getClass());
								for (Method method:sourceMethods) {
									if (method.getName().startsWith("get")) {
										if (method.getName().equals("getClass")) {
											continue;
										}
										Object loValue = method.invoke(obj);
										if (CommonUtil.objectIsNotNull(loValue)) {
											strBuilder.append(" AND ").append(entry.getKey()).append(".")
													.append(CommonUtil.getFieldName(method.getName()))
													.append("=?");
											valuesList.add(loValue);
										}
									}
								}
							}
						}
					}
					if (CommonUtil.objectIsNotNull(otherConditions)) {
						for (String otherCondition : otherConditions) {
							if (CommonUtil.objectIsNotNull(otherCondition)) {
								strBuilder.append(" AND ").append(otherCondition);
							}
						}
					}
					return new Object[] { strBuilder.toString(), valuesList };
				}

			} else {
				Set<Entry<String, Object>> tempSet = aliasTableMap.entrySet();
				if (aliasEntityMap == null || aliasEntityMap.size() == 0) {
					int tableSize = aliasTableMap.size();
					if (CommonUtil.objectIsNull(selectFields)) {
						if (tableSize == 1) {
							strBuilder.append("SELECT * FROM ").append(aliasTableMap.values().toArray()[0]);
						} else {
							strBuilder.append("SELECT ");
							int i = 0;
							for (Entry<String, Object> entry : tempSet) {
								if (i == 0) {
									strBuilder.append(entry.getKey()).append(".*");
								} else {
									strBuilder.append(",").append(entry.getKey()).append(".*");
								}
								i++;
							}
							strBuilder.append(" FROM ");
							i = 0;
							for (Entry<String, Object> entry : tempSet) {
								if (i == 0) {
									strBuilder.append(entry.getValue()).append(" ").append(entry.getKey());
								} else {
									strBuilder.append(",").append(entry.getValue()).append(" ").append(entry.getKey());
								}
								i++;
							}
						}
					} else {
						strBuilder.append("SELECT ").append(selectFields).append(" FROM ");
						if (tableSize == 1) {
							strBuilder.append(aliasTableMap.values().toArray()[0]);
						} else {
							int i = 0;
							for (Entry<String, Object> entry : tempSet) {
								if (i == 0) {
									strBuilder.append(entry.getValue()).append(" ").append(entry.getKey());
								} else {
									strBuilder.append(",").append(entry.getValue()).append(" ").append(entry.getKey());
								}
								i++;
							}
						}
					}
					strBuilder.append(" WHERE 1=1");
					if (CommonUtil.objectIsNotNull(otherConditions)) {
						for (String otherCondition : otherConditions) {
							if (CommonUtil.objectIsNotNull(otherCondition)) {
								strBuilder.append(" AND ").append(otherCondition);
							}
						}
					}
					return new Object[] { strBuilder.toString(), valuesList };
				} else {
					ArrayList<String> aliasList = new ArrayList<String>();
					for (String key : aliasTableMap.keySet()) {
						aliasList.add(key);
					}
					for (String key : aliasEntityMap.keySet()) {
						if (!aliasList.contains(key)) {
							aliasList.add(key);
						}
					}
					int tableSize = aliasList.size();
					if (CommonUtil.objectIsNull(selectFields)) {
						if (tableSize == 1) {
							strBuilder.append("SELECT * FROM ");
							if (CommonUtil.objectIsNotNull(aliasTableMap.values().toArray()[0])) {
								strBuilder.append(aliasTableMap.values().toArray()[0]);
							} else {
								strBuilder.append(aliasEntityMap.values().toArray()[0].getClass().getSimpleName());
							}
						} else {
							strBuilder.append("SELECT ");
							int i = 0;
							for (String key : aliasList) {
								if (i == 0) {
									strBuilder.append(key).append(".*");
								} else {
									strBuilder.append(",").append(key).append(".*");
								}
								i++;
							}
							strBuilder.append(" FROM ");
							i = 0;
							for (String key : aliasList) {
								if (i != 0) {
									strBuilder.append(",");
								}
								if (aliasTableMap.get(key) != null) {
									strBuilder.append(aliasTableMap.get(key));
								} else {
									strBuilder.append(aliasEntityMap.get(key).getClass().getSimpleName());
								}
								strBuilder.append(" ").append(key);
								i++;
							}
						}
					} else {
						strBuilder.append("SELECT ").append(selectFields).append(" FROM ");
						if (tableSize == 1) {
							if (CommonUtil.objectIsNotNull(aliasTableMap.values().toArray()[0])) {
								strBuilder.append(aliasTableMap.values().toArray()[0]);
							} else {
								strBuilder.append(aliasEntityMap.values().toArray()[0].getClass().getSimpleName());
							}
						} else {
							int i = 0;
							for (String key : aliasList) {
								if (i != 0) {
									strBuilder.append(",");
								}
								if (aliasTableMap.get(key) != null) {
									strBuilder.append(aliasTableMap.get(key));
								} else {
									strBuilder.append(aliasEntityMap.get(key).getClass().getSimpleName());
								}
								strBuilder.append(" ").append(key);
								i++;
							}
						}
					}
					strBuilder.append(" WHERE 1=1");
					if (tableSize == 1) {
						Object obj = aliasEntityMap.values().toArray()[0];
						if (obj != null) {
							Set<Method> sourceMethods = BeanHelper.getAllMethods(obj.getClass());
							for (Method method:sourceMethods) {
								if (method.getName().startsWith("get")) {
									if (method.getName().equals("getClass")) {
										continue;
									}
									Object loValue = method.invoke(obj);
									if (CommonUtil.objectIsNotNull(loValue)) {
										strBuilder.append(" AND ")
												.append(CommonUtil.getFieldName(method.getName()))
												.append("=?");
										valuesList.add(loValue);
									}
								}
							}
						}
					} else {
						for (Entry<String, Object> entry : aliasEntityMap.entrySet()) {
							Object obj = entry.getValue();
							if (obj != null) {
								Set<Method> sourceMethods = BeanHelper.getAllMethods(obj.getClass());
								for (Method method:sourceMethods) {
									if (method.getName().startsWith("get")) {
										if (method.getName().equals("getClass")) {
											continue;
										}
										Object loValue = method.invoke(obj);
										if (CommonUtil.objectIsNotNull(loValue)) {
											strBuilder.append(" AND ").append(entry.getKey()).append(".")
													.append(CommonUtil.getFieldName(method.getName()))
													.append("=?");
											valuesList.add(loValue);
										}
									}
								}
							}
						}
					}
					if (CommonUtil.objectIsNotNull(otherConditions)) {
						for (String otherCondition : otherConditions) {
							if (CommonUtil.objectIsNotNull(otherCondition)) {
								strBuilder.append(" AND ").append(otherCondition);
							}
						}
					}
					return new Object[] { strBuilder.toString(), valuesList.toArray() };
				}
			}
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static final Object[] getSelectSql(String tableName, Map<String, Object> conditions) {
		StringBuilder sqlBuilder = new StringBuilder("select * from ");
		sqlBuilder.append(tableName);
		Object[] params;
		if (conditions != null && conditions.size() > 0) {
			params = new Object[conditions.size()];
			sqlBuilder.append(" where ");
			int i = 0;
			for (Map.Entry<String, Object> keyValue : conditions.entrySet()) {
				sqlBuilder.append(keyValue.getKey()).append("=? and ");
				params[i++] = keyValue.getValue();
			}
		} else {
			params = new Object[0];
		}
		return new Object[] {
				params.length > 0 ? sqlBuilder.substring(0, sqlBuilder.length() - 5) : sqlBuilder.toString(), params };
	}

	public static final Object[] getInsertSql(String tableName, Map<String, Object> values) {
		StringBuilder sqlBuilder = new StringBuilder("insert into ");
		sqlBuilder.append(tableName).append(" (");
		Object[] params;
		int length = 0;
		if (values != null && values.size() > 0) {
			params = new Object[values.size()];
			for (Map.Entry<String, Object> keyValue : values.entrySet()) {
				sqlBuilder.append(keyValue.getKey()).append(",");
				params[length++] = keyValue.getValue();
			}
			sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
		} else {
			params = new Object[0];
		}
		sqlBuilder.append(") values (");
		if (length > 0) {
			while ((length--) > 0) {
				sqlBuilder.append("?,");
			}
			sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
		}
		sqlBuilder.append(")");
		return new Object[] { sqlBuilder.toString(), params };
	}

	public static final Object[] getUpdateSql(String tableName, Map<String, Object> values,
			Map<String, Object> conditions) {
		StringBuilder sqlBuilder = new StringBuilder("update ");
		sqlBuilder.append(tableName);
		Object[] params;
		int length = 0;
		if (values != null && values.size() > 0) {
			length += values.size();
		}
		if (conditions != null && conditions.size() > 0) {
			length += conditions.size();
		}

		params = new Object[length];
		int i = 0;
		if (values != null && values.size() > 0) {
			sqlBuilder.append(" set ");
			for (Map.Entry<String, Object> keyValue : values.entrySet()) {
				sqlBuilder.append(keyValue.getKey()).append("=?,");
				params[i++] = keyValue.getValue();
			}
			sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
		}
		if (conditions != null && conditions.size() > 0) {
			sqlBuilder.append(" where ");
			for (Map.Entry<String, Object> keyValue : conditions.entrySet()) {
				sqlBuilder.append(keyValue.getKey()).append("=? and ");
				params[i++] = keyValue.getValue();
			}
			sqlBuilder.delete(sqlBuilder.length() - 4, sqlBuilder.length());
		} else {
			params = new Object[0];
		}
		return new Object[] { sqlBuilder.toString(), params };
	}

	public static final Object[] getDeleteSql(String tableName, Map<String, Object> conditions) {
		StringBuilder sqlBuilder = new StringBuilder("delete from ");
		sqlBuilder.append(tableName);
		Object[] params;
		int length = 0;
		if (conditions != null && conditions.size() > 0) {
			sqlBuilder.append(" where ");
			params = new Object[conditions.size()];
			for (Map.Entry<String, Object> keyValue : conditions.entrySet()) {
				sqlBuilder.append(keyValue.getKey()).append("=? and ");
				params[length++] = keyValue.getValue();
			}
			sqlBuilder.delete(sqlBuilder.length() - 4, sqlBuilder.length());
		} else {
			params = new Object[0];
		}
		return new Object[] { sqlBuilder.toString(), params };
	}
}
