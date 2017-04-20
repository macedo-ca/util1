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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.compress.utils.IOUtils;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import ca.macedo.stores4j.BinStores.BinStore;
import parts4j.PartRegistry;
import parts4j.Part;
import parts4j.PartType;

@SuppressWarnings("rawtypes")
public class AvroObject<T>{
	private static enum Cdc{gzip2,snappy}
	
	static PartType<AvroObject> partType=PartRegistry.register(AvroObject.class,"utility");
	static Executor defExecutor=partType.using(Executors.newCachedThreadPool());
	Part part=partType.register(this, this::getClassName);
	
	Executor executor=defExecutor;
	Class<T> clz=null;
	Schema sch=null;
	Cdc codec=null;
	long feederTimeout=30000;
	DecoderFactory decFact=DecoderFactory.get();
	
	public String getClassName(){
		return clz.getName();
	}
	
	BinStore store=null;
	public AvroObject<T> useStore(BinStore store){
		this.store=store;
		return this;
	}
	
	public static <T> AvroObject<T> byClass(Class<T> clz, Schema sch){
		AvroObject<T> com=new AvroObject<T>();
		com.clz=clz;
		com.sch=sch;
		return com;
	}
	public static <T> AvroObject<T> byClass(Class<T> clz){
		AvroObject<T> com=new AvroObject<T>();
		com.clz=clz;
		com.sch=ReflectData
				//.AllowNull
				.get()
				.getSchema(clz);
		return com;
	}
	
	public Writer<T> write(T t) throws Exception {
	    Writer<T> w=new Writer<T>();
		w.write(t);
		return w;
	}
	public Writer<T> writeBatchTo(AvroPushReceiver<T> t) throws IOException {
	    Writer<T> w=new Writer<T>();
		w.writeTo(t);
		w.setBatch();
		return w;
	}
	public Writer<T> writeBatchTo(OutputStream out) throws IOException {
	    Writer<T> w=new Writer<T>();
		w.output=out;
		w.setBatch();
		return w;
	}
	public Writer<T> writeTo(AvroPushReceiver<T> t) throws IOException {
	    Writer<T> w=new Writer<T>();
		w.writeTo(t);
		return w;
	}
	public Writer<T> writeBatchToStore(String id) throws IOException {
	    return writeBatchTo(store.item(id).setContentStream());
	}
	public Reader readBatchFromStore(String id) throws IOException{
		Reader out=new Reader();
		out.src=store.item(id).getContentStream();
		out.batch=true;
		return out;
	}
	public Reader read(){
		Reader out=new Reader();
		return out;
	}
	public Reader readBase64(String base){
		Reader out=new Reader();
		out.src=new ByteArrayInputStream(Base64.getUrlDecoder().decode(base));
		return out;
	}
	public Reader readBatch(){
		Reader out=new Reader();
		out.batch=true;
		return out;
	}
	public Reader readBatch(byte[] ar){
		Reader out=new Reader();
		out.batch=true;
		out.src=new ByteArrayInputStream(ar);
		return out;
	}
	public Reader read(byte[] ar){
		Reader out=new Reader();
		out.src=new ByteArrayInputStream(ar);
		return out;
	}
	public Reader readBatchFromDB(final PreparedStatement ps) throws Exception{
		try(ResultSet rs = ps.executeQuery()){
			if(rs.next()){
				return readBatchFromDB(rs,1);
			}else{
				return null;
			}
		}
	}
	public Reader readFromDB(final PreparedStatement ps) throws SQLException{
		try(ResultSet rs = ps.executeQuery()){
			return readFromDB(rs,1);
		}
	}
	public Reader readFromDB(final ResultSet rs){
		return readFromDB(rs,1);
	}
	public Reader readBatchFromDB(final ResultSet rs, final int column) throws Exception{
		Reader out=new Reader();
		out.src=readDBField(rs,column);
		out.batch=true;
		return out;
	}
	private InputStream readDBField(final ResultSet rs, final int column) throws SQLException, IOException {
		Object o=rs.getObject(column);
		if(o instanceof String){
			return new ByteArrayInputStream(Base64.getDecoder().decode(((String) o)));
		}else if(o instanceof byte[]){
			return new ByteArrayInputStream((byte[])o);
		}else if(o instanceof Blob){
			return ((Blob)o).getBinaryStream();
		}
		return null;
	}
	public Reader readFromDB(final ResultSet rs, final int column){
		Reader out=new Reader();
		AvroPullFeed<T> feed=new AvroPullFeed<T>(){
			@Override
			public byte[] pull() throws Exception {
				if(rs.next()){
					InputStream is=readDBField(rs, column);
					return toBytes(is);
				}
				return null;
			}
		};
		out.pull=feed;
		return out;
	}
	public byte[] toBytes(InputStream is) throws IOException{
		ByteArrayOutputStream bos=new ByteArrayOutputStream();
		IOUtils.copy(is, bos);
		return bos.toByteArray();
	}
	
	public interface AvroPushReceiver<T>{
		public void onEntry(byte[] data, boolean last) throws Exception;
	}
	public interface AvroPullFeed<T>{
		public byte[] pull() throws Exception;
	}
	public interface ReadVisitor<T>{
		void visit(T item, int row);
	}
	public class Reader{
		boolean batch=false;
		Exception error=null;
		AvroPushReceiver<T> feed=null;
		AvroPullFeed<T> pull=null;
		InputStream src=null;
		byte[] preRetrieved=null;
		BinaryDecoder _decoder =null;
		DataFileStream<T> batchSource=null;
		SpecificDatumReader<T> reader=null;
		Reader(){
		    reader= new ReflectDatumReader<T>(clz);
			reader.setSchema(sch);
		}
		
		boolean gotLast=false;
		LinkedBlockingQueue<byte[]> buffer=null;
		int row=0;
		public AvroPushReceiver<T> receiver(){
			buffer=new LinkedBlockingQueue<byte[]>();
			feed = (bytes,last)->{
				buffer.add(bytes);
				gotLast=last;
			};
			return feed;
		}
		public Reader forEach(ReadVisitor<T> readVisit) throws Exception{
			row=0;
			while(next()){
				readVisit.visit(read(), row);
				row++;
			}
			return this;
		}
		public Boolean next(){
			if(batch){
				if(batchSource==null && src!=null){
					try {
						batchSource=new DataFileStream<T>(src, reader);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				if(batchSource!=null && batchSource.hasNext()){
					return true;
				}
			}
			if(pull!=null){
				try {
					preRetrieved=pull.pull();
					return preRetrieved!=null;
				} catch (Exception e) {
					error=e;
					return false;
				}
			}
			if(feed==null) return false;
			if(buffer!=null && gotLast && buffer.size()==0){
				return false;
			}
			preRetrieved= poll();
			return true;
		}
		private byte[] poll(){
			try {
				byte[] sourceData=buffer.poll(feederTimeout,TimeUnit.MILLISECONDS) ;
				return sourceData;
			} catch (InterruptedException e) {
				return null;
			}
		}
		@SuppressWarnings("resource")
		private InputStream getStream() throws IOException{
			InputStream out=null;
			if(src!=null){
				out=src;
			}else{
				out=new ByteArrayInputStream(getBytes());
			}
			if(codec==Cdc.snappy){
			    out = new SnappyInputStream(out);
			}
			return out;
		}
		private byte[] getBytes(){
			if(preRetrieved!=null){
				byte[] out=preRetrieved;
				preRetrieved=null;
				return out;
			}
			return poll();
		}
		public T read() throws Exception{
			if(error!=null) throw error;
			if(batch) return readFromBatch();
			try(InputStream str=getStream()){
//			    byte[] bytes = getBytes();
//				if(codec==Cdc.snappy){
//				    bytes = Snappy.uncompress(bytes);
//				}
				_decoder = decFact.binaryDecoder(str, _decoder);
				T out=reader.read(null, _decoder);
				return out;
			}
		}
		private T readFromBatch() throws IOException {
			if(batchSource==null){
			    byte[] bytes = getBytes();
				if(bytes!=null){
					batchSource=new DataFileStream<T>(new ByteArrayInputStream(bytes), reader);
				}
			}
			if(batchSource!=null && batchSource.hasNext()){
				return batchSource.next();
			}
			return null;
		}
	}
	
	public class Writer<I> implements Closeable{
		boolean eachRow=true;
		DatumWriter<I> writer = null;
		DataFileWriter<I> batchWriter = null;
	    ByteArrayOutputStream bos=null;
	    OutputStream output=null;
	    OutputStream outputOther=null;
	    BinaryEncoder encoder = null;
		boolean writerPrepped=false;
		AvroPushReceiver<I> receiver=null;
		long counter=0;
		boolean hasEntryToWrite=false;
		
		public Writer<I> setBatch() throws IOException{
			eachRow=false;
			batchWriter=new DataFileWriter<I>(writer=new ReflectDatumWriter<I>(sch));
			batchWriter.setCodec((codec==Cdc.snappy)?CodecFactory.snappyCodec():null);
			if(output!=null){
				if(codec == Cdc.snappy){
					outputOther=output;
					output= new SnappyOutputStream(output);
			    }
				batchWriter.create(sch, output);
			}else{
				batchWriter.create(sch, bos=new ByteArrayOutputStream() );
			}
			writerPrepped=true;
			return this;
		}
		private void write0(I data) throws Exception{
			if(batchWriter!=null){
				batchWriter.append(data);
			}else{
				writer.write(data, encoder);
			}
		}
		
		public long getCounter(){
			return counter;
		}
		public Writer<I> writeTo(AvroPushReceiver<I> t) {
			receiver=t;
			return this;
		}
		@SuppressWarnings("unchecked")
		private void prepWriter(){
		    if(writerPrepped) return;
		    if(writer==null) writer = (ReflectDatumWriter<I>)new ReflectDatumWriter<T>(clz);
		    bos=new ByteArrayOutputStream();
		    encoder = EncoderFactory.get().binaryEncoder(bos, encoder);
		    writerPrepped=(receiver==null || (!eachRow));
		}
		public Writer<I> write(I t) throws Exception{
			checkAndWriteOutput(); // from last added
			counter++;
			prepWriter();
			write0(t);
			hasEntryToWrite=true;
			return this;
		}
		
		public String toBase64() throws Exception{
			return Base64.getUrlEncoder().encodeToString(toBytes());
		}
		public byte[] toBytes() throws Exception{
			if(!hasEntryToWrite) return null;
			if(encoder!=null) encoder.flush();
			if(batchWriter!=null) batchWriter.flush();
			bos.flush();
		    byte[] bytes= bos.toByteArray();
		    if(encoder!=null){
			    if(codec == Cdc.gzip2){
			    	// TODO
			    }else if(codec == Cdc.snappy){
			    	bytes = Snappy.compress(bytes);
			    }
		    }
		    return bytes;
		}
		public void close(){
			try {
				closeWriter();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		private void checkAndWriteOutput() throws Exception{
			if(receiver!=null && hasEntryToWrite){
				if(receiver!=null){
					if(eachRow || closing){
						receiver.onEntry(toBytes(), closing);
						hasEntryToWrite=false;
					}
				}else if(output!=null){
					try {
						output.flush();
					} catch (Throwable e) {
						e.printStackTrace();
					}
					try {
						output.close();
					} catch (Throwable e) {
						e.printStackTrace();
					}
					
				}
			}
			if(closing && outputOther!=null){
				try{outputOther.close();outputOther=null;}catch(Throwable e){}
			}
		}
		boolean closing=false;
		public AvroObject<T> closeWriter() throws Exception{
			closing=true;
			checkAndWriteOutput();
			if(bos!=null) bos.close();
			hasEntryToWrite=false;
			return (AvroObject<T>)AvroObject.this;
		}
	}
	public AvroObject<T> close(Connection con,PreparedStatement ps) throws Exception{
		try{if(ps!=null)ps.close();}catch(Throwable t){}
		try{if(con!=null)con.close();}catch(Throwable t){}
		return AvroObject.this;
	}
	public AvroObject<T> close(PreparedStatement ps) throws Exception{
		try{if(ps!=null)ps.close();}catch(Throwable t){}
		return AvroObject.this;
	}
	public AvroObject<T> close(Closeable ... closables) throws Exception{
		for(Closeable c : closables) try{if(c!=null)c.close();}catch(Throwable t){}
		return AvroObject.this;
	}

	public long getFeederTimeout() {
		return feederTimeout;
	}
	public AvroObject<T> setFeederTimeout(long feederTimeout) {
		this.feederTimeout = feederTimeout;
		return this;
	}
	public Executor getExecutor() {
		return executor;
	}
	public AvroObject<T> setExecutor(Executor executor) {
		this.executor = executor;
		return this;
	}
	public Cdc getCodec(){
		return codec;
	}
	public AvroObject<T> setCodec(String name){
		codec = Cdc.valueOf(name);
		return this;
	}
	public AvroObject<T> setCodec(Cdc name){
		codec=name;
		return this;
	}

	public AvroObject<T> useSnappy(){
		setCodec(Cdc.snappy);
		return this;
	}
}