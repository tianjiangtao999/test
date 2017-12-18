package com.cmcc.redis;

import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;

public class RClient {
	private Jedis jedis = new Jedis("127.0.0.1");

	public void testRedisPing() {
		System.out.println(jedis.ping());
	}

	public void testRedisKeys() {
		Set<String> keyset = jedis.keys("*");
		for (String key : keyset) {
			System.out.println(key);
		}
	}

	public void testRedisHash() {
		Map<String, String> map = jedis.hgetAll("/usr/local/games/src.txt");
		for (String key : map.keySet()) {
			System.out.println(key + "\t" + map.get(key));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RClient rc = new RClient();
		rc.testRedisPing();
		rc.testRedisKeys();
		rc.testRedisHash();
	}

}
