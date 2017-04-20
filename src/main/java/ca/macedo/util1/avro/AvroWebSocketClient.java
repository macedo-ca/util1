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
package ca.macedo.util1.avro;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketByteListener;
import org.asynchttpclient.ws.WebSocketPingListener;
import org.asynchttpclient.ws.WebSocketPongListener;
import org.asynchttpclient.ws.WebSocketTextListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import ca.macedo.util1.avro.AvroObject.AvroPushReceiver;
import parts4j.PartRegistry;
import parts4j.Part;
import parts4j.PartType;

@SuppressWarnings("rawtypes")
public class AvroWebSocketClient<I,O> implements Closeable{
	static PartType<AvroWebSocketClient> partType = PartRegistry.register(AvroWebSocketClient.class);
	
	public static <I,O> Bldr<I,O> from(String url, Class<I> fromType, Class<O> toType){
		return new Bldr<I,O>(url);
	}
	public static <I,O> Bldr<I,O> from(String url, AvroObject<I> fromType, AvroObject<O> toType){
		return new Bldr<I,O>(url).in(fromType).out(toType);
	}
	public static class Bldr<I,O>{
		Bldr(String u){
			url=u;
		}
		String url=null;
		AvroObject<I> avroIn=null;
		AvroObject<O> avroOut=null;
		boolean perThread=true;
		
		public Bldr<I,O> perThread(boolean perThread){
			this.perThread=perThread;
			return this;
		}
		public Bldr<I,O> in(AvroObject<I> o){
			avroIn=o;
			return this;
		}
		public Bldr<I,O> out(AvroObject<O> o){
			avroOut=o;
			return this;
		}
		
		public AvroWebSocketClient<I,O> build(){
			AvroWebSocketClient<I,O> out = new AvroWebSocketClient<I,O>(url);
			out.avroIn=avroIn;
			out.avroOut=avroOut;
			try {
				out.cl=Dsl.asyncHttpClient();
				out.handler=new Handler<O>();
				WebSocketUpgradeHandler uh=new WebSocketUpgradeHandler.Builder().addWebSocketListener(out.handler).build();
				out.ws= out.cl.prepareGet(url).execute(uh).get();
			} catch (ExecutionException e) {
				out.error=new RuntimeException(e.getCause());
			} catch (RuntimeException e) {
				out.error=e;
			}catch(Exception e){
				out.error=new RuntimeException(e);
			}
			return out;
		}
	}
	
	Part<AvroWebSocketClient> part = null;
	AvroObject<I> avroIn=null;
	AvroObject<O> avroOut=null;
    AsyncHttpClient cl=null;
    WebSocket ws=null;
    String url=null;
	Handler<O> handler=null;
	boolean closed=false;
	RuntimeException error=null;
	
	public String getUrl(){
		return url;
	}
	public RuntimeException getError(){
		return error;
	}
	public AvroWebSocketClient(String u){
		url=u;
		part = partType
				.register(this, this::getUrl)
					.registerErrors(this::getError)
		;
	}
	
	public O request(I in){
		if(error!=null) throw error;
		AvroObject<O>.Reader r=null;
		long n=System.nanoTime();
		try {
			r=avroOut.read();
			handler.rec=r.receiver();
			avroIn.writeTo((bytes,last)->{
				ws.sendMessage(bytes);
			}).write(in).close();
			return r.read();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}finally{
			System.out.println((System.nanoTime()-n)/1000000.0);
		}
	}
	
	public void write(I in){
		if(error!=null) throw error;
		try {
			avroIn.writeTo((bytes,last)->{
				ws.sendMessage(bytes);
			}).write(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public AvroObject<I>.Writer<I> writer() throws IOException{
		if(error!=null) throw error;
		return avroIn.writeBatchTo((bytes,last)->{
			ws.sendMessage(bytes);
		});
	}
	/**
	 * Blocking read one, waits for single record
	 * @return
	 */
	public O read() throws Exception{
		if(error!=null) throw error;
		AvroObject<O>.Reader r=avroOut.read();
		handler.rec=r.receiver();
		return r.read();
	}
	
	public AvroObject<O>.Reader reader() throws Exception {
		if(error!=null) throw error;
		AvroObject<O>.Reader r=avroOut.readBatch();
		handler.rec=r.receiver();
		return r;
	}
	
	@Override
	public void close() {
		closed=true;
		try {
			if(ws!=null) ws.close();
		} catch (Throwable e) {
		}
		ws=null;
		try{
			if(cl!=null) cl.close();
		} catch (Throwable e) {
		}
		cl=null;
	}
	
	private static class Handler<O> implements WebSocketByteListener, WebSocketTextListener, WebSocketPingListener, WebSocketPongListener {
		AvroPushReceiver<O> rec=null;
	    @Override
	    public void onMessage(byte[] message) {
	    	try {
				if(rec!=null) rec.onEntry(message, true);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }
	    @Override
	    public void onPing(byte[] message) {}
	    @Override
	    public void onPong(byte[] message) {}
	    @Override
	    public void onMessage(String message) {}
	    @Override
	    public void onOpen(WebSocket websocket) {}
	    @Override
	    public void onClose(WebSocket websocket) {}
	    @Override
	    public void onError(Throwable t) {}
	}
}
