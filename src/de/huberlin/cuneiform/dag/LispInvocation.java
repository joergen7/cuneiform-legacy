/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Jörgen Brandt (HU Berlin)
 * Marc Bux (HU Berlin)
 * Ulf Leser (HU Berlin)
 *
 * Jörgen Brandt is funded by the European Commission through the BiobankCloud
 * project. Marc Bux is funded by the Deutsche Forschungsgemeinschaft through
 * research training group SOAMED (GRK 1651).
 *
 * Copyright 2014 Humboldt-Universität zu Berlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package de.huberlin.cuneiform.dag;

import de.huberlin.cuneiform.common.ForeignLangCatalog;

public class LispInvocation extends Invocation {
	
	private static final String LISP_SHEBANG = "#!/usr/bin/env clisp\n";

	public LispInvocation( TaskNode taskNode ) throws NotDerivableException {
		super( taskNode );
	}

	@Override
	public String getShebang() {
		return LISP_SHEBANG;
	}

	@Override
	public String varDef( String varname, DataList list )
	throws NotDerivableException {

		String ret;
		boolean comma;
		int i;
		
		ret = "(defparameter "+varname+" '(";
		
		comma = false;
		for( i = 0; i < list.size(); i++ ) {
			
			if( comma )
				ret += " ";
			
			comma = true;
			
			ret += "\""+list.get( i ).getValue()+"\"";
		}
		
		ret += "))\n";
		
		return ret;
	}

	@Override
	public String varDef( String varname, String value ) {
		return "(defparameter "+varname+" \""+value+"\")\n";
	}


	@Override
	public String getCheckPost() {
		return "";
	}

	@Override
	public String callFunction(String name, String... argValue) {
		
		StringBuffer buf;
		
		buf = new StringBuffer();
		
		buf.append( '(' ).append( name );
		
		for( String value : argValue )
			buf.append( ' ' ).append( value );
		
		buf.append( ')' );
		
		return buf.toString();
	}

	@Override
	public String defFunction(String funName, String outputName,
			String[] inputNameList, String body) {
		
		String ret;
		boolean comma;
		
		ret = "(defun "+funName+" (";
		
		comma = false;
		for( String invar : inputNameList ) {
			
			if( comma )
				ret += " ";
			
			comma = true;
			
			ret += invar;
		}
		
		ret += ")\n  "+body+")\n";
		
		return ret;
	}

	@Override
	public String defFunctionLog() throws NotDerivableException {
		return defFunction( FUN_LOG, null, new String[] { "key", "value" },
			"(or (stringp key) (error \"Parameter 'key' must be of type string.\"))\n"
			+"(or (stringp value) (error \"Parameter 'value' must be of type string.\"))\n"
			+"(with-open-file (outstream \""+REPORT_FILENAME
			+"\" :direction :output :if-exists :append :if-does-not-exist "
			+":create)\n(format outstream \"~D000|"+getDagId()+"|"+getWfName()
			+"|"+getTaskNodeId()+"|"+getSignature()+"|"+getTaskName()
			+"|~A|~A~%\" (- (get-universal-time) (* (+ (* 70 365) 17) 24 "
			+"3600)) key value))" );
	}

	@Override
	public String defFunctionLogUsr() {
		return defFunction(
			FUN_USERLOG,
			null,
			new String[] { "value" },
			"("+FUN_LOG+" "+JsonReportEntry.KEY_INVOC_USER+" value)" );
	}

	@Override
	public String newList( String listName ) {
		return "(defparameter "+listName+" ())\n";
	}

	@Override
	public String listAppend( String listName, String element ) {
		return "(setf "+listName+" (cons "+element+" "+listName+"))\n";
	}

	@Override
	public String dereference( String varName ) {
		return varName;
	}

	@Override
	public String forEach( String listName, String elementName, String body ) {
		return "(dolist ("+elementName+" "+listName+") "+body+")\n";
	}

	@Override
	public String ifNotFileExists( String fileName, String body ) {
		
		return "(if (probe-file "+fileName+") "+body+")\n";
	}

	@Override
	public String raise( String msg ) {
		return "(error "+msg+")\n";
	}

	@Override
	public String join( String... elementList ) {
		
		StringBuffer buf;
		
		buf = new StringBuffer();
		
		buf.append( "(format nil \"~{~a~}\"" );
		
		for( String element : elementList )
			buf.append( ' ' ).append( element );
		
		buf.append( ")" );
		
		return buf.toString();
	}

	@Override
	public String quote( String content ) {
		return "\""+content+"\"";
	}

	@Override
	public String fileSize( String filename ) {
		return "(file-size (pathname "+filename+"))";
	}

	@Override
	public String ifListIsNotEmpty( String listName, String body ) {
		return "(if (null "+listName+") body)\n";
	}

	@Override
	public String listToBraceCommaSeparatedString(String listName,
		String stringName, String open, String close) {
		
		return "(defparameter "+stringName+" (format nil \"~a~{~a~^,~}~a\" "
			+open+" "+listName+" "+close+"))\n";
	}

	@Override
	public String defFunctionNormalize() throws NotDerivableException {
		return defFunction(
			FUN_NORMALIZE,
			null,
			new String[] { "channel", "f" },
			"(format nil \""+getSignature()
			+"_~d_~s\" channel (file-namestring (pathname f)))\n" );
	}

	@Override
	public String symlink( String src, String dest ) {
		return "(shell (format nil \"ln -s ~a ~a\" "+src+" "+dest+"))";
	}

	@Override
	public String getImport() {
		return "";
	}
	
	@Override
	public String comment( String comment ) {
		return "; "+comment.replace( "\n", "\n# " )+"\n";
	}

	@Override
	public String copyArray( String from, String to ) {
		return "(defparameter "+to+" "+from+")\n";
	}

	@Override
	public int getLangId() {
		return ForeignLangCatalog.LANGID_LISP;
	}

}
