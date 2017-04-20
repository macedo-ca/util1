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
import java.util.Collection;

import ca.macedo.stores4j.TextStores;
import ca.macedo.stores4j.Extensions.BaseExtention;
import ca.macedo.stores4j.TextStores.TextStore;
import ca.macedo.util1.CamelUtil.HasWrappedCloseable;
import ca.macedo.util1.redis.Redis.RedisKeyRef;
import redis.clients.jedis.Jedis;

public class RedisTextStore extends TextStore implements HasWrappedCloseable{
	
	public static BaseExtention<TextStore, TextStores> extention=new BaseExtention<TextStore,TextStores>() {
		@Override
		public boolean isMine(TextStores stores, String uri) {
			return uri!=null && uri.toLowerCase().startsWith("redis:");
		}
		@Override
		public TextStore get(TextStores stores,String uri) {
			uri=TextStores.after(uri,"redis:");
			String key =null;
			Redis r=null;
			if(uri.lastIndexOf('#')>-1){
				key = uri.substring(uri.lastIndexOf('#')+1);
				uri = uri.substring(0,uri.lastIndexOf('#'));
				r=((Redis)stores.getFactory().apply(Redis.class,uri));
			}else{
				key = uri;
				r=((Redis)stores.getFactory().apply(Redis.class,new String[]{"redis","redis_texts"}));
			}
			return new RedisTextStore(r.getKeyRef(key));
		}
	};
	
	/**
	 * By providing the un-wrapping, the Redis instance is only closed once
	 */
	@Override
	public Closeable getWrappedCloseable() {
		return keyref.getParent();
	}
	/**
	 * Convenience method, but is not called by CamelUtil to close the object, the un-wrapped Redis instance is closed instead
	 */
	@Override
	public void close() {
		super.close();
		keyref.stop();
	}
	
	public RedisTextStore(RedisKeyRef k){
		keyref=k;
	}
	RedisKeyRef keyref=null;
	@Override
	public Collection<String> list() {
		try(Jedis j = keyref.getRedis()){
			Collection<String> out=j.hkeys(keyref.getKey());
			return out;
		}
	}
	@Override
	public boolean has(String id) {
		try(Jedis j = keyref.getRedis()){
			return j.hexists(keyref.getKey(), id);
		}
	}
	@Override
	public TextRef item(final String id) {
		String key=keyref.getKey();
		return new TextRef(){
			@Override
			public String getID() {
				return id;
			}
			@Override
			public String getContent() {
				try(Jedis j = keyref.getRedis()){
					return j.hget(key, id);
				}
			}
			@Override
			public boolean createWithContent(String content) {
				try(Jedis j = keyref.getRedis()){
					return j.hsetnx(key, id, content)==1;
				}
			}
			@Override
			public void setContent(String content) {
				try(Jedis j = keyref.getRedis()){
					j.hset(key, id, content);
				}
			}
			@Override
			public boolean rename(String newID) {
				try(Jedis j = keyref.getRedis()){
					String content=j.hget(key, id);
					if(content!=null){
						j.hdel(key, id);
						j.hset(key, newID, content);
						return true;
					}else{
						return false;
					}
				}
			}
			@Override
			public boolean delete(String delete) {
				try(Jedis j = keyref.getRedis()){
					return j.hdel(key, id)>0;
				}
			}
		};
	}
}