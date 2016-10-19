package com.efun.general;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.NOPLogger;

/**
 * 
 * 日志记录器（用 slf4j 实现）
 *
 */
public class LogUtil
{
	/** JessMA 日志对象名称 */
	public static final String LOGGER_NAME	= "EFUN";
	/** JessMA 日志对象 */
	public static final Logger logger;
	
	static
	{
		logger = LoggerFactory.getLogger(LOGGER_NAME);
		
		if(logger == NOPLogger.NOP_LOGGER)
			System.err.println("!!! --> EFUN Logger is not valid, please check <-- !!!");
	}
	
	/** 获取 JessMA 日志记录器对象 */
	public static final Logger getLogger()
	{
		return logger;
	}
	
	/** 获取日志记录器对象 */
	public static final Logger getLogger(Class<?> clazz)
	{
		return LoggerFactory.getLogger(clazz);
	}
	
	/** 获取日志记录器对象 */
	public static final Logger getLogger(String name)
	{
		return LoggerFactory.getLogger(name);
	}
	
	/** 记录操作异常日志 */
	public static final void exception(Throwable e, Object action, boolean printStack)
	{
		exception(logger, e, action, printStack);
	}
	
	/** 记录操作异常日志 */
	public static final void exception(Logger logger, Throwable e, Object action, boolean printStack)
	{
		StringBuilder msg = new StringBuilder("Execption occur on ");
		msg.append(action);
		msg.append(" -> ");
		msg.append(e.toString());
		String error = msg.toString();

		if(printStack)
			logger.error(error, e);
		else
			logger.error(error);
	}
	
	/** 记录操作失败日志 */
	public static final void fail(Object action, Object module, boolean printStack)
	{
		fail(logger, action, module, printStack);
	}
	
	/** 记录操作失败日志 */
	public static final void fail(Logger logger, Object action, Object module, boolean printStack)
	{
		StringBuilder msg = new StringBuilder("Operation fail on ");
		msg.append(action);
		msg.append(" -> ");
		msg.append(module);

		logger.error(msg.toString());
	}
	
	/** 记录服务器启动日志 */
	public static final void logServerStartup(Object o)
	{
		logger.info("->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->->");
		logger.info("starting: {}", o);
	}
	
	/** 记录服务器关闭日志 */
	public static final void logServerShutdown(Object o)
	{
		logger.info("stoping: {}", o);
		logger.info("<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-<-");
	}
}
