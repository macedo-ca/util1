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
package ca.macedo.util1;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Service;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.DefaultComponent;
import org.apache.camel.impl.SimpleRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.macedo.stores4j.TextStores;
import ca.macedo.stores4j.TextStores.TextStore;
import ca.macedo.util1.factory.Factory;
import ca.macedo.util1.factory.Singleton;
import ca.macedo.util1.factory.SettingsParser.NeedsSettings;
import ca.macedo.util1.factory.SettingsParser.Settings;

/**
 * Central Camel utility for dealing with life-cycle start/stop, singleton creation for Factory, SettingsParser etc
 */
public class CamelUtil extends DefaultComponent{
	private static Logger log=LoggerFactory.getLogger(CamelUtil.class);
	public CamelUtil(){}
	public CamelUtil(CamelContext ctx){
		super(ctx);
	}
	public TextStores getStores(){
		return stores.get();
	}
	public Factory getFactory(){
		return factory.get();
	}
	
	// No endpoint can be created from this component
	@Override protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception { throw new RuntimeException("Not supported"); }
	
	private HashSet<WeakReference<Closeable>> toClose=new HashSet<WeakReference<Closeable>>();
	
	public interface HasWrappedCloseable{
		public Closeable getWrappedCloseable();
	}
	
	public static <C extends Closeable> C ensureClosed(CamelContext ctx, C c){
		getInstance(ctx).ensureClosed(c);
		return c;
	}
	
	public CamelUtil ensureClosed(Closeable ... closable){
		for(Closeable c : closable ){
			if(c instanceof HasWrappedCloseable){
				c=((HasWrappedCloseable)c).getWrappedCloseable();
			}
			toClose.add(new WeakReference<Closeable>(c));
		}
		return this;
	}
	@Override
	protected void doStop() throws Exception {
		super.doStop();
		log.info("Shutting down"); 
		WeakReference<Closeable> c =null;
		long out=0;
		LinkedList<String> types=new LinkedList<String>();
		while(toClose.size()>0){
			c= toClose.iterator().next();
			toClose.remove(c);
			Closeable cl = c.get();
			out++;
			if(cl!=null){
				try{cl.close();}catch(Throwable t){}
				types.add(cl.getClass().getName());
			}
		}
		log.info("Shut down, including "+ out + " related objects "+types+"."); 
	}
	
	public static CamelUtil getInstance(CamelContext ctx){
		CamelUtil h = (CamelUtil)ctx.getComponent("CamelUtilHelp");
		if(h==null) h=create(ctx);
		return h;
	}
	private static synchronized CamelUtil create(CamelContext ctx){
		CamelUtil h = (CamelUtil)ctx.getComponent("CamelUtilHelp");
		if(h==null){
			log.info("Started");
			ctx.addComponent("CamelUtilHelp", h=new CamelUtil(ctx));
		}
		return h;
	}
	
	public static Factory getFactory(CamelContext ctx){
		return getInstance(ctx).getFactory();
	}
	
	
	public static void setSettings(CamelContext ctx, String component, String instance, NeedsSettings inst){
		getFactory(ctx).getSettings().setSettings(component, instance, inst);
	}
	public static TextStore getTextStore(CamelContext ctx, String setting){
		String propVal=ctx.getProperty(setting);
		return ensureClosed(ctx,getTextStores(ctx).getStore(propVal));
	}
	public static TextStores getTextStores(CamelContext ctx){
		CamelUtil help=new CamelUtil();
		return help.getStores();
	}
	public static TextStore getTextStoreFromSettings(CamelContext ctx, Settings props, String property, String defaultPropValue){
		return ensureClosed(ctx,getTextStores(ctx).getStore(props.getStr(property,defaultPropValue)));
	}
	
	Singleton<TextStores> stores=Singleton.from(TextStores.class,()->new TextStores());
	Singleton<Factory> factory=Singleton.from(Factory.class,()->{
		CamelContext ctx=getCamelContext();
		Factory f=new Factory();
		try {
			ctx.addService(new Service() {
				@Override public void stop() throws Exception { f.stop();}
				@Override public void start() throws Exception {}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		f.setContext(CamelContext.class, ctx);
		final SimpleRegistry sr=ctx.getRegistry(SimpleRegistry.class);
		if(sr!=null){
			f.setDefaultCache(sr);
			sr.put("_factory", f);
		}
		f.getSettings().setSettingsSource((component,instance)-> new CamelPropertiesComponentSettings(ctx));
		return f;
	});
	
	/**
	 * Ties properties from CamelContext (actually the Camel PropertiesComponent) to Settings object by providing them as a PropertySource
	 */
	public static class CamelPropertiesComponentSettings extends Settings{
		public CamelPropertiesComponentSettings(CamelContext ctx){
			this.ctx=ctx;
		}
		CamelContext ctx=null;
		@Override
		public boolean contains(String prop) {
			return get(prop,null)!=null;
		}
		@Override
		public Object get(String prop, Object defaultValue) {
			try {
				Object o= ctx.getComponent("properties",PropertiesComponent.class).parseUri("{{"+prop+"}}");
				return (o==null) ? defaultValue : o;
			} catch (Exception e) {
				return defaultValue;
			}
		}
	}
	
	
}
