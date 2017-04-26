package com.efun.mainland.util.redis;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * redis3服务器端集群
 * 
 * @author Administrator
 *
 */
public class RedisCluster extends JedisCluster {


	public RedisCluster(HostAndPort node) {
		super(node);
	}

	public RedisCluster(HostAndPort node, int timeout) {
		super(node, timeout);
	}

	public RedisCluster(HostAndPort node, int timeout, int maxAttempts) {
		super(node, timeout, maxAttempts);
	}

	public RedisCluster(HostAndPort node, GenericObjectPoolConfig poolConfig) {
		super(node, poolConfig);
	}

	public RedisCluster(HostAndPort node, int timeout, GenericObjectPoolConfig poolConfig) {
		super(node, timeout, poolConfig);
	}

	public RedisCluster(HostAndPort node, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
		super(node, timeout, maxAttempts, poolConfig);
	}

	public RedisCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
		super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
	}

	public RedisCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
		super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
	}

	public RedisCluster(Set<HostAndPort> nodes) {
		super(nodes);
	}

	public RedisCluster(Set<HostAndPort> nodes, int timeout) {
		super(nodes, timeout);
	}

	public RedisCluster(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
		super(nodes, timeout, maxAttempts);
	}

	public RedisCluster(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
		super(nodes, poolConfig);
	}

	public RedisCluster(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
		super(nodes, timeout, poolConfig);
	}

	public RedisCluster(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, timeout, maxAttempts, poolConfig);
	}

	public RedisCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
	}

	public RedisCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
	}

	/**
	 * throw new IOException("RedisCluster Not Supported:close()");
	 */
	@Override
	public void close() throws IOException {
		throw new IOException("RedisCluster Not Supported:close()");
	}

	protected void destroy() throws IOException {
		super.close();
	}

}
