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

import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import parts4j.util.Readers;

public class CFHelp {

	/**
	 * Reads the regular Cloud Foundry services configuration from the system-environment variables, or a class-path resource called 
	 * VCAP_SERVICES.JSON
	 * @return
	 */
	public static HashMap<String,JSONObject> getCloudFoundryServiceConfiguration(){
		HashMap<String,JSONObject> names=new HashMap<String,JSONObject>();
		String svcs=System.getenv("VCAP_SERVICES");
		JSONObject VCAP_SERVICES =null;
		try {
			if(svcs==null || svcs.length()==0){
				System.out.println("VCAP_SERVICES not defined, loading VCAP_SERVICES.JSON");
				VCAP_SERVICES = parts4j.util.Readers.fromResource("VCAP_SERVICES.JSON").onErrException().toJSONObject();
			}else{
				VCAP_SERVICES = parts4j.util.Readers.fromSystemEnv("VCAP_SERVICES").onErrException().toJSONObject();
			}
			for(Object type : VCAP_SERVICES.keySet()){
				JSONArray svcType=(JSONArray)VCAP_SERVICES.get(type);
				for(int i=0;i<svcType.size();i++){
					JSONObject svc = (JSONObject)svcType.get(i);
					names.put((String)svc.get("name"),svc);
				}
			}
		} catch (Throwable e) {
			throw new RuntimeException("VCAP_SERVICES env data failed to load",e);
		}
		return names;
	}

	/**
	 * Reads the regular Cloud Foundry services configuration from the system-environment variables, or a class-path resource called 
	 * VCAP_SERVICES.JSON
	 * @return
	 */
	public static HashMap<String,JSONObject> getCloudFoundryServiceCredentialsAsJSONObjects(){
		HashMap<String,JSONObject> names=new HashMap<String,JSONObject>();
		String svcs=System.getenv("VCAP_SERVICES");
		JSONObject VCAP_SERVICES =null;
		try {
			if(svcs==null) return new JSONObject();
			VCAP_SERVICES = (JSONObject)new JSONParser().parse(svcs);
			for(Object type : VCAP_SERVICES.keySet()){
				JSONArray svcType=(JSONArray)VCAP_SERVICES.get(type);
				for(int i=0;i<svcType.size();i++){
					JSONObject svc = (JSONObject)svcType.get(i);
					names.put((String)svc.get("label"),(JSONObject)svc.get("credentials"));
				}
			}
		} catch (Throwable e) {
			throw new RuntimeException("VCAP_SERVICES env data failed to load",e);
		}
		return names;
	}
}
