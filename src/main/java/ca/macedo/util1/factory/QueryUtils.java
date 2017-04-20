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

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility to parse query-strings for parameters
 */
public class QueryUtils {

    public static Map<String, Object> parseQuery(String uri, boolean useRaw, boolean lenient) throws URISyntaxException {
        if (!lenient) {
            if (uri != null && uri.endsWith("&")) {
                throw new URISyntaxException(uri, "Invalid uri syntax: Trailing & marker found. "
                        + "Check the uri and remove the trailing & marker.");
            }
        }

        if (uri == null || uri.trim().length()==0) {
            return new LinkedHashMap<String, Object>(0);
        }
        try {
            Map<String, Object> rc = new LinkedHashMap<String, Object>();

            boolean isKey = true;
            boolean isValue = false;
            boolean isRaw = false;
            StringBuilder key = new StringBuilder();
            StringBuilder value = new StringBuilder();

            for (int i = 0; i < uri.length(); i++) {
                char ch = uri.charAt(i);
                char next;
                if (i <= uri.length() - 2) {
                    next = uri.charAt(i + 1);
                } else {
                    next = '\u0000';
                }
                if (isKey && ch == '=') {
                    isKey = false;
                    isValue = true;
                    isRaw = false;
                    continue;
                }
                if (ch == '&') {
                    addParam(key.toString(), value.toString(), rc, useRaw || isRaw);
                    key.setLength(0);
                    value.setLength(0);
                    isKey = true;
                    isValue = false;
                    isRaw = false;
                    continue;
                }
                if (isKey) {
                    key.append(ch);
                } else if (isValue) {
                    value.append(ch);
                }
            }
            if (key.length() > 0) {
                addParam(key.toString(), value.toString(), rc, useRaw || isRaw);
            }
            return rc;

        } catch (UnsupportedEncodingException e) {
            URISyntaxException se = new URISyntaxException(e.toString(), "Invalid encoding");
            se.initCause(e);
            throw se;
        }
    }
    private static final String CHARSET = "UTF-8";

    @SuppressWarnings("unchecked")
	private static void addParam(String name, String value, Map<String, Object> map, boolean isRaw) throws UnsupportedEncodingException {
        name = URLDecoder.decode(name, CHARSET);
        if (!isRaw) {
            String s = replaceAll(value, "%", "%25");
            value = URLDecoder.decode(s, CHARSET);
        }
        if (map.containsKey(name)) {
            Object existing = map.get(name);
            List<String> list;
            if (existing instanceof List) {
                list = (List<String>) existing;
            } else {
                list = new ArrayList<String>();
                String s = existing != null ? existing.toString() : null;
                if (s != null) {
                    list.add(s);
                }
            }
            list.add(value);
            map.put(name, list);
        } else {
            map.put(name, value);
        }
    }
    private static String replaceAll(String input, String from, String to) {
        if (input==null || input.trim().length()==0) {
            return input;
        }
        if (from == null) {
            throw new IllegalArgumentException("from cannot be null");
        }
        if (to == null) {
            throw new IllegalArgumentException("to cannot be null");
        }

        if (!input.contains(from)) {
            return input;
        }

        final int len = from.length();
        final int max = input.length();
        StringBuilder sb = new StringBuilder(max);
        for (int i = 0; i < max;) {
            if (i + len <= max) {
                String token = input.substring(i, i + len);
                if (from.equals(token)) {
                    sb.append(to);
                    i = i + len;
                    continue;
                }
            }
            sb.append(input.charAt(i));
            i++;
        }
        return sb.toString();
    }
}
