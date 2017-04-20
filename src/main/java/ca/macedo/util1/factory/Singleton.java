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

import java.util.function.Supplier;

public class Singleton<S> {
	Class<S> clz=null;
	Supplier<S> factory=null;
	volatile S volInst=null;
	S fastInst=null;
	
	public void set(S setValue){
		volInst=setValue;
		fastInst=setValue;
	}
	
	public S get(){
		if(fastInst!=null) return fastInst;
		if(volInst!=null) return fastInst=volInst;
		synchronized(this){
			if(volInst==null){
				volInst=factory.get();
			}
			return fastInst=volInst;
		}
	}
	public static <S> Singleton<S> from(Class<S> clz, Supplier<S> factory){
		Singleton<S> sg=new Singleton<S>();
		sg.factory=factory;
		sg.clz=clz;
		return sg;
	}

	public boolean canReturn(Class<?> anotherClz) {
		return clz.isAssignableFrom(anotherClz);
	}
}
