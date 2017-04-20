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
package ca.macedo.util1.factory;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.macedo.util1.factory.SettingsParser.NeedsSettings;

/**
 * Simple factory class that configures objects and works with cache.
 * Supports creating instances from Cloud-foundry VCAP env-variables, query-string format, and query-string format with host-name.
 * Examples<BR>
 * <li> redis-server-host-name?port=1234&password=235sdfsd 
 */
public class Factory implements LifeCycle{
	private static Logger log=LoggerFactory.getLogger(Factory.class);
	Map<String,JSONObject> cfServiceCreds=CFHelp.getCloudFoundryServiceCredentialsAsJSONObjects();
	static Factory inst=new Factory();
	
	private LinkedList<LifeCycle> stoppables=new LinkedList<LifeCycle>();
	
	@Override
	public void start() {}
	@Override
	public void stop() {
		for(LifeCycle c : stoppables){
			try {
				c.stop();
			} catch (Throwable e) {
			}
		}
	}
	
	Map<String,Object> defaultCache=null;
	SettingsParser settings=new SettingsParser(this);
	
	public SettingsParser getSettings() {
		return settings;
	}
	public void setSettings(SettingsParser settings) {
		this.settings = settings;
	}
	public Map<String, Object> getDefaultCache() {
		return defaultCache;
	}
	public void setDefaultCache(Map<String, Object> defaultCache) {
		this.defaultCache = defaultCache;
	}
	
	Object ctx=null;
	public <T> void setContext(Class<T> type, T o){
		ctx=o;
	}
	@SuppressWarnings("unchecked")
	public <T> T getContext(Class<T> type){
		return (T)ctx;
	}
	
	public class FactoryRequest<T>{
		FactoryRequest(Class<T> c){
			clz=c;
		}
		Class<T> clz=null;
		
		Object sourceObject=null;
		String hostnameParam="hostname";
		Map<Object,Object> cache=null;
		String[] possibleNames=null;
		public FactoryRequest<T> configureWith(Object source){
			sourceObject=source;
			return this;
		}
		@SuppressWarnings("unchecked")
		public FactoryRequest<T> useCache(Map<?,?> cache){
			this.cache=(Map<Object,Object>)cache;
			return this;
		}
		public FactoryRequest<T> possibleNames(String ...strings){
			this.possibleNames=strings;
			return this;
		}
		public FactoryRequest<T> queryHostnameIsParam(String hostNameParam) {
			this.hostnameParam=hostNameParam;
			return this;
		}
		public T create(){
			if(possibleNames!=null){
				for(String nm : possibleNames){
					T r=createBean(cache,clz,((Object)nm), true,hostnameParam,false);
					if(r!=null) return r;
				}
			}else if(sourceObject!=null){
				T r=createBean(cache,clz,sourceObject, true,hostnameParam,false);
				if(r!=null) return r;
			}
			return null;
		}
	}
	public <T> FactoryRequest<T> startCreating(Class<T> c, HashMap<?,?> ca){
		FactoryRequest<T> t=new FactoryRequest<T>(c).useCache(ca);
		return t;
	}
	public <T> FactoryRequest<T> startCreating(Class<T> c){
		FactoryRequest<T> t=new FactoryRequest<T>(c);
		return t;
	}
	
	/**
	 * Creates new object instance of basic configurable object using provided cache
	 * @param cache
	 * @param clz
	 * @param from
	 * @param addToCache
	 * @param baseName
	 * @param throwExcep
	 * @return
	 */
	private <T> T createBean(Map<Object,Object> cache, Class<T> clz, Object from, boolean addToCache, String baseName, boolean throwExcep){
		if(from instanceof Singleton && ((Singleton)from).canReturn(clz)) return (T)((Singleton<T>)from).get();
		if(from instanceof NeedsSettings) return (T)from;
		Object cached=cache!=null ? cache.get(from) : null;
		if(cached!=null && cached instanceof NeedsSettings) return (T)cached;
		Map<String,Object> map=null;
		T pcfg =null;
		try{
			pcfg = clz.newInstance();
			if(pcfg instanceof LifeCycle){
				stoppables.add((LifeCycle)pcfg);
			}
		} catch (Throwable e) {
			throw new RuntimeException("Could not create "+clz, e);
		}
		if(from instanceof String){
			String s=((String)from);
			if(cfServiceCreds.containsKey(from)){
				map=cfServiceCreds.get(from);
			}else if(baseName!=null){
				try{
					int idx=s.indexOf('?');
					if(idx==-1){
						if(!throwExcep) return null;
						throw new RuntimeException("Invalid URI ("+s+"), syntax should be uri with parameters ("+baseName +"?p1=v1[&pX=vX])");
					}
					String host=s.substring(0, idx);
					String q=s.substring(idx+1);
					map=QueryUtils.parseQuery(q,false,false);
					map.put(baseName, host);
				} catch (URISyntaxException e) {
					throw new RuntimeException("Invalid URI ("+from+"), syntax should be uri with parameters ("+baseName +"?p1=v1[&pX=vX])");
				}
			}else{
				try{
					map=QueryUtils.parseQuery(s,false,false);
				} catch (URISyntaxException e) {
					throw new RuntimeException("Invalid query-string ("+from+"), syntax should uri-parameters (p1=v1[&pX=vX])");
				}
			}
		}else if(from instanceof JSONObject){
			map=((JSONObject)from);
		}
		if(pcfg instanceof NeedsSettings) ((NeedsSettings)pcfg).setSettings(SettingsParser.getDefaultSource(map));
		if(addToCache && cache!=null) cache.put(from, pcfg);
		if(pcfg instanceof LifeCycle){
			try {
				((LifeCycle)pcfg).start();
			} catch (Throwable e) {
				log.error("Factory bean could not be started",e);
			}
		}
		return pcfg;
	}

	

}
