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
package ca.macedo.util1.excel;

import java.io.FileInputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFName;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFTable;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Excel query utility
 */
public class ExcelQueryParser {
	private static Logger log=LoggerFactory.getLogger(ExcelQueryParser.class);
	public static void main(String[] ar) throws Throwable{
		ExcelQueryParser p= new ExcelQueryParser();
		
		System.out.println(p.parseQuery("SELECT * FROM [A3:Z33] hdr=no"));
		System.out.println(p.parseQuery("SELECT * FROM [sht1!A1:B10] hdr=no"));
		System.out.println(p.parseQuery("SELECT * FROM [Sheet1!A1:B10] hdr=yes"));
		
		ParsedQuery q = p.parseQuery("SELECT * FROM Table2 FILES /tmp/");
		System.out.println(q);
		q = p.parseQuery("SELECT F1 as cool1, F2 as cool2, F3 FROM [Sheet1!A1:B10] FILES c:/tmp/data*.xlsx");
		System.out.println(q);
		
    	try(XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream("C:/tmp/Book1.xlsx"))){
        	System.out.println(p.parseQuery("SELECT * FROM [C16:D21] HDR=no").prepare(workbook));
        	System.out.println(p.parseQuery("SELECT * FROM table2").prepare(workbook));
        	System.out.println(p.parseQuery("SELECT * FROM test1").prepare(workbook));
    	}
   	}
	
	public ParsedQuery parseQuery(String sql){
		return parseQuery0(sql);
	}
	
	interface ParserToken{
		default void parse(String part, ParsedQuery q){}
	}
	enum ExSQLToken implements ParserToken{
		SELECT,
		fieldnames{ @Override public void parse(String part, ParsedQuery q) {
			if(part.trim().equals("*")){
				q.all=true;
			}else{
				q.fieldSelection=part.trim().split(",");
			}
		}},
		FROM,
		tbl{@Override public void parse(String part, ParsedQuery q) {
			part=part.trim();
			if(part.startsWith("[")){
				part=part.substring(1,part.length()-1);
				q.setFormula(part);
			}else{
				q.rangeName=part;
			}
		}},
		HDR,
		hdrvalue{@Override public void parse(String part, ParsedQuery q) {
			part=part.trim();
			if(part.equalsIgnoreCase("no") || part.equalsIgnoreCase("off") || part.equalsIgnoreCase("false")){
				q.headerRow=false;
			}
		}},
		FILES,
		filesselection{@Override public void parse(String part, ParsedQuery q) {
			part=part.trim();
			q.files=new ExcelFiles(part);
		}}
	}
	
	public static class PreparedQuery extends ParsedQuery{
		LinkedHashMap<String,Integer> fieldSelectionMap=null;
		Integer[] cols=null;
		String[] names=null;
		boolean skipEmptyRows=true;
		public boolean isSkipEmptyRows() {
			return skipEmptyRows;
		}
		public void setSkipEmptyRows(boolean skipEmptyRows) {
			this.skipEmptyRows = skipEmptyRows;
		}
		public PreparedQuery preparedQuery(){
			return this;
		}
		public PreparedQuery forEachCol(BiConsumer<Integer,String> colv){
			for(int i=0;i<cols.length;i++){
				colv.accept(cols[i], names[i]);
			}
			return this;
		}
		
		public boolean isFound(){
			return (start!=null && end!=null);
		}
		
		PreparedQuery preProcess(XSSFWorkbook book){
			if(rangeName!=null){
				if(rangeName.indexOf('!')>-1){
					sheet = rangeName.substring(0, rangeName.indexOf('!'));
					rangeName=rangeName.substring(rangeName.indexOf('!')+1);
					List<XSSFName> n=book.getNames(rangeName);
			        if(n!=null) for(XSSFName ran : n){
			        	if(ran.getSheetName().equalsIgnoreCase(sheet)){
				        	setFormula(ran.getRefersToFormula());
							rangeName=null;
			        	}
			        }
				}else{
					XSSFTable t =book.getTable(rangeName);
					if(t!=null){
						sheet		= t.getSheetName();
						start		= t.getStartCellReference().formatAsString();
						end			= t.getEndCellReference().formatAsString();
						headerRow	= true;
						rangeName	= null;
					}else{
						XSSFName n=book.getName(rangeName);
				        if(n!=null){
				        	setFormula(n.getRefersToFormula());
							rangeName=null;
				        }
					}
				}
			}
			preProcessFields(book);
			return this;
		}
		public PreparedQuery visitAll(XSSFWorkbook book, Consumer<XSSFRow> rowV){
			if(!isFound()) return this;
			XSSFSheet sh = sheet!=null ? book.getSheet(sheet) : book.getSheetAt(0);
			CellReference startCR = new CellReference(start);
			CellReference endCR = new CellReference(end);
			int rowNum=startCR.getRow() + (headerRow?1:0);
			int skipped=0;
			while(rowNum<endCR.getRow()){
				XSSFRow row= sh.getRow(rowNum);
				if(row!=null || !skipEmptyRows){
					rowV.accept(row);
				}else{
					skipped++;
				}
				if(skipped>50){
					if(sh.getLastRowNum()<rowNum) return this;
				}
				rowNum++;
			}
			return this;
		}
		private PreparedQuery preProcessFields(XSSFWorkbook book){
			if(!isFound()) return this;
			XSSFSheet sh = sheet!=null ? book.getSheet(sheet) : book.getSheetAt(0);
			CellReference startCR = new CellReference(start);
			CellReference endCR = new CellReference(end);
			XSSFRow row= sh.getRow(startCR.getRow());
			int startCol = startCR.getCol();
			int endCol = endCR.getCol();
			fields = new String[(endCol-startCol)+1];
			if(!headerRow){
				for(int i=startCol;i<endCol+1;i++){
					fields[i-startCol]="F"+(i-startCol+1);
				}
			}else{
				try {
					for(int i=startCol;i<endCol+1;i++){
						fields[i-startCol]=(row.getCell(i).getStringCellValue());
					}
				} catch (Exception e) {
					log.warn("Parsing header row threw an error, perhaps this is a header-less excel range?",e);
				}
			}
			preProcessFieldSelection(startCol,endCol);
			return this;
		}
		private PreparedQuery preProcessFieldSelection(int startCol,int endCol) {
			int colCount=0;
			if(all){
				fieldSelection=fields;
				colCount=fieldSelection.length;
				names=fieldSelection;
				cols=new Integer[colCount];
				for(int i=startCol;i<endCol+1;i++){
					cols[i-startCol]=i;
				}
			}else{
				fieldSelectionMap=new LinkedHashMap<>();
				for(int i=0;i<fieldSelection.length;i++){
					String fld=fieldSelection[i].trim();
					String src=fld, name=fld;
					int idx=fld.toLowerCase().indexOf(" as ");
					if(idx>-1){
						src=fld.substring(0, idx);
						name=fld.substring(idx+" as ".length());
					}
					Integer fidx=indexOf(src,fields);
					if(fidx!=null){
						fieldSelectionMap.put(name, fidx);
					}
				}
				colCount=fieldSelectionMap.size();
				cols=fieldSelectionMap.values().toArray(new Integer[colCount]);
				names=fieldSelectionMap.keySet().toArray(new String[colCount]);
			}
			return this;
		}
	}
	public static class ExcelFiles{
		String pfx=null;
		public String getPfx() {
			return pfx;
		}
		public String getSfx() {
			return sfx;
		}

		String sfx=null;
		ExcelFiles(String fls){
			int idx=fls.indexOf('*');
			if(idx>-1){
				pfx=fls.substring(0, idx);
				sfx=fls.substring(idx+1);
			}else{
				pfx=fls;
			}
		}
		
		public String toString(){
			return ((pfx!=null ? pfx : "") +"*" + (sfx!=null ? sfx : ""));
		}
	}
	public static class ParsedQuery{
		boolean all=false;
		String[] fields=null;
		String[] fieldSelection=null;
		boolean headerRow=true;
		String sheet=null;
		String start=null;
		String end=null;
		String rangeName=null;
		ExcelFiles files=null;
		
		public ExcelFiles files(){
			return files;
		}
		
		public PreparedQuery prepare(XSSFWorkbook book){
			PreparedQuery q=new PreparedQuery();
			q.all=all;
			q.fields=fields;
			q.fieldSelection=fieldSelection;
			q.headerRow=headerRow;
			q.sheet=sheet;
			q.start=start;
			q.end=end;
			q.rangeName=rangeName;
			q.files=files;
			return q.preProcess(book);
		}
		
		void setFormula(String formula){
			int idx=(formula.indexOf('!'));
			if(idx>-1){
				sheet=formula.substring(0, idx);
				if(sheet.startsWith("'") && sheet.endsWith("'")) sheet=sheet.substring(1, sheet.length()-1);
				formula=formula.substring(idx+1);
			}
			idx = formula.indexOf(':');
			if(idx>-1){
				start=formula.substring(0,idx);
				end=formula.substring(idx+1);
			}else{
				rangeName=formula;
			}
		}
		public String toString(){
			return "SELECT " + 
					(fieldSelection!=null ? toStr(fieldSelection) : fields!=null ? toStr(fields) : "*") + 
					" FROM " + (rangeName!=null ? rangeName : "["+(sheet!=null?sheet+"!":"")+start+":"+end+"]") +
					" "+(headerRow ? "HDR=Yes":"HDR=No")
					+ (files!=null?" FILES "+files:"")
					;
		}
	}
	
	//// Parsing method
	private ParsedQuery parseQuery0(String sql){
		if(sql==null) return null;
		sql=sql.trim();
		String[] parts=sql.replace('=',' ').split(" ");
		
		ParsedQuery q = new ParsedQuery();
		
		ExSQLToken[] tokens=ExSQLToken.values();
		int curr = 0;
		String concatParts="";
		for(String p : parts){
			if(p!=null && p.trim().length()>0){
				if( tokens[curr]==ExSQLToken.tbl && p.equalsIgnoreCase("FILES")){
					tokens[curr].parse(concatParts, q);
					curr=curr+4;
					concatParts="";
				}else if( p.equalsIgnoreCase( tokens[curr].name() ) ){
					tokens[curr].parse(concatParts, q);
					curr=curr+1;
					concatParts="";
				}else if( concatParts.length()>0 && p.equalsIgnoreCase( tokens[curr+1].name() ) ){
					tokens[curr].parse(concatParts, q);
					curr=curr+2;
					concatParts="";
				}else{
					if(tokens[curr].equals(ExSQLToken.fieldnames) && p.equalsIgnoreCase("as")){
						concatParts+=" AS ";
					}else{
						concatParts+=p;
					}
				}
			}
		}
		tokens[curr].parse(concatParts, q);
		return q;
	}

	

	/// Utilities
	static Integer indexOf(String val, String[] in){
		for(int i=0;i<in.length;i++){
			if(val.equalsIgnoreCase(in[i])) return i;
		}
		return null;
	}
	private static String toStr(String[] ar){
		String out="";
		for(String s : ar){
			if(out.length()>0) out+=",";
			out+=s;
		}
		return out;
	}

}
