/******************************************************************************
 *  Copyright (c) 2017 Johan Macedo
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 *  Contributors:
 *    Johan Macedo
 *****************************************************************************/
package ca.macedo.util1.redis;

import java.io.Closeable;
import java.net.URISyntaxException;
import java.util.Map;

import ca.macedo.stores4j.TextStores;
import ca.macedo.util1.factory.LifeCycle;
import ca.macedo.util1.factory.QueryUtils;
import ca.macedo.util1.factory.SettingsParser;
import ca.macedo.util1.factory.SettingsParser.NeedsSettings;
import ca.macedo.util1.factory.SettingsParser.Settings;
import ca.macedo.util1.factory.SettingsParser.SettingsProvider;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * General utility to work with Redis, through the Jedis library
 */
public class Redis implements NeedsSettings, LifeCycle, Closeable{
	protected JedisPoolConfig poolConfig=new JedisPoolConfig();
	protected Settings cfg=null;
	protected JedisPool pool=null;
	
	static{
		TextStores.EXTENSIONS.register(RedisTextStore.extention);
	}
	
	public Redis(){}
	public Redis(String uri){
		int idx=uri.indexOf('?');
		if(idx==-1){
			throw new RuntimeException("Invalid URI ("+uri+"), syntax should be uri with parameters ([hostname]?password=v1[&port=vX][&timeout=vX])");
		}
		try {
			String host=uri.substring(0, idx);
			String q=uri.substring(idx+1);
			Map<String,Object> map=QueryUtils.parseQuery(q,false,false);
			map.put("hostname", host);
			setSettings(SettingsParser.getDefaultSource(map));
		} catch (URISyntaxException e) {
			throw new RuntimeException("Invalid URI ("+uri+"), syntax should be uri with parameters ([hostname]?password=v1[&port=vX][&timeout=vX])",e);
		}
		start();
	}
	
	public class RedisKeyRef implements SettingsProvider{
		String key=null;
		RedisKeyRef(String k){
			key=k;
		}
		public String getKey(){
			return key;
		}
		public Jedis getRedis(){
			return Redis.this.getRedis();
		}
		public Redis getParent(){
			return Redis.this;
		}
		
		public void stop(){
			Redis.this.stop();
		}
		@Override
		public Settings getSettings() {
			return new RedisKeyRefPropertySource(this);
		}
	}
	public RedisKeyRef getKeyRef(String key){
		return new RedisKeyRef(key);
	}
	public static class RedisKeyRefPropertySource extends Settings{
		RedisKeyRef kref=null;
		public RedisKeyRefPropertySource(RedisKeyRef k){
			kref=k;
		}
		@Override
		public boolean contains(String prop) {
			try(Jedis j = kref.getRedis()){
				return j.hexists(kref.key, prop);
			}
		}
		@Override
		public Object get(String prop, Object defaultValue) {
			try(Jedis j = kref.getRedis()){
				Object out=j.hget(kref.key, prop);
				return (out!=null) ? out : defaultValue;
			}
		}
	}
	
	@Override
	public void setSettings(Settings source){
		if(pool!=null && source.shouldReplace(cfg)){
			cfg=source;
			JedisPool pool2 = new JedisPool(poolConfig
					, cfg.getStr("hostname", null)
					, cfg.getInt("port", Protocol.DEFAULT_PORT)
					, cfg.getInt("timeout", Protocol.DEFAULT_TIMEOUT)
					, cfg.getStr("password", null)
			);
			JedisPool oldPool=pool;
			pool=pool2;
			try {
				oldPool.destroy();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else{
			cfg=source;
		}
	}
	public void start(){
		if(pool!=null) return;
		pool = new JedisPool(poolConfig
				, cfg.getStr("hostname", null)
				, cfg.getInt("port", Protocol.DEFAULT_PORT)
				, cfg.getInt("timeout", Protocol.DEFAULT_TIMEOUT)
				, cfg.getStr("password", null)
		);
	}
	
	public Jedis getRedis(){
		return pool.getResource();
	}
	
	public void stop(){
		if(pool!=null) pool.destroy();
	}
	@Override
	public void close() {
		stop();
	}
}
