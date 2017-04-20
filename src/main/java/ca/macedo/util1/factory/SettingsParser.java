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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import ca.macedo.stores4j.TextStores.TextStore;

/**
 * Multi-source settings parser, resolver and access mechanism. Supports accessing settings from Properties, Map,
 * JSON-object, and JS-object in a uniform way.<br>
 * This was created to ensure that components that need properties do not need to implement various way of reading simple settings,
 * such as from CloudFoundry JSON object and properties files.
 */
public class SettingsParser {
	static SettingsParser inst=new SettingsParser();
	public static Settings getDefaultSource(Object o){
		return inst.getSettings(o);
	}
	public SettingsParser(){}
	public SettingsParser(Factory factory){
		this.factory=factory;
	}
	
	public interface NeedsSettings{
		public void setSettings(Settings settings);
	}
	public interface SettingsSource{
		public Settings getSettings(String component, String instance);
	}
	
	
	Factory factory=null;
	TextStore store=null;
	
	SettingsSource settingsSource=null;
	
	public SettingsSource getSettingsSource() {
		return settingsSource;
	}
	public void setSettingsSource(SettingsSource settingsSource) {
		this.settingsSource = settingsSource;
	}
	public void setSettings(String component, String instance, NeedsSettings componentInstance){
		if(settingsSource==null) throw new RuntimeException("A settingsSource must be set by calling setSettingsSource(...) first");
		Settings source=settingsSource.getSettings(component,instance);
		componentInstance.setSettings(source);
	}
	
	@SuppressWarnings("unchecked")
	public Settings getSettings(Object o){
		if(o instanceof Map<?,?>){
			return new MapSettings(((Map<String,Object>)o));
		}else if(o instanceof Properties){
			return new PropertiesSettings(((Properties)o));
		}else if(o instanceof SettingsProvider){
			return ((SettingsProvider)o).getSettings();
		}
		return null;
	}
	public interface SettingsProvider{
		public Settings getSettings();
	}
	
	/**
	 * Flat settings object, that is in essence a wrapper for Properties like objects, but 
	 * also supports other sources. This object is also used as primary means of managing 
	 * re-configuration of services and instances.
	 */
	public abstract static class Settings{
		String name=null;
		public String getName(){
			return name;
		}
		public void assertHas(String err, String ... props){
			for(String p : props){
				if(!contains(p)) throw new RuntimeException(err);
			}
		}
		long ts=System.currentTimeMillis();
		public boolean shouldReplace(Settings oldSource){
			return oldSource==null || oldSource.ts < ts;
		}
		public abstract boolean contains(String prop);
		public abstract Object get(String prop, Object defaultValue);
		public Integer getInt(String prop, Integer defaultValue){
			try {
				Object o=get(prop,defaultValue);
				return o!=null ? o instanceof Number ? ((Number)o).intValue() :  Integer.parseInt(o.toString()) : defaultValue;
			} catch (NumberFormatException e) {
				return defaultValue;
			}
		}
		public Double getDouble(String prop, Double defaultValue){
			try {
				Object o=get(prop,defaultValue);
				return o!=null ? o instanceof Number ? ((Number)o).doubleValue() :  Double.parseDouble(o.toString()) : defaultValue;
			} catch (NumberFormatException e) {
				return defaultValue;
			}
		}
		public String getStr(String prop, String defaultValue){
			Object o=get(prop,defaultValue);
			return o!=null ? o instanceof String ? ((String)o) : o.toString() : defaultValue;
		}
		public Boolean getBool(String prop, Boolean defaultValue){
			Object o = get(name, defaultValue);
			if(o instanceof Boolean) return (Boolean)o;
			if(o instanceof Number) return ((Number)o).intValue()!=0;
			return o!=null ? TRUES.indexOf(o.toString().toLowerCase())>-1 : defaultValue;
		}
		private static final List<Object> TRUES = Arrays.asList(new Object[]{"true","yes","y","1","-1"});
	}
	public static class MapSettings extends Settings{
		Map<String,Object> m;
		public MapSettings(Map<String,Object> map){
			m=map;
			name="map";
		}
		@Override
		public boolean contains(String prop) {
			return m.containsKey(prop);
		}
		@Override
		public Object get(String prop, Object defaultValue) {
			return m.getOrDefault(prop, defaultValue);
		}
	}
	public static class PropertiesSettings extends Settings{
		Properties p;
		public PropertiesSettings(Properties props){
			p=props;
			name="properties";
		}
		@Override
		public boolean contains(String prop) {
			return p.containsKey(prop);
		}
		@Override
		public Object get(String prop, Object defaultValue) {
			return p.getProperty(prop, defaultValue!=null?defaultValue.toString():null);
		}
	}
	
	public Factory getFactory() {
		return factory;
	}
	public void setFactory(Factory factory) {
		this.factory = factory;
	}
	public TextStore getStore() {
		return store;
	}
	public void setStore(TextStore store) {
		this.store = store;
	}
	
}
