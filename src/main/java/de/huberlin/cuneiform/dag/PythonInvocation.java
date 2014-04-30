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

public class PythonInvocation extends Invocation {

	protected PythonInvocation( TaskNode taskNode ) throws NotDerivableException {
		super( taskNode );
	}

	@Override
	public String callFunction( String name, String... argValue ) {
		
		StringBuffer buf;
		boolean comma;
		
		buf = new StringBuffer();
		
		buf.append( name ).append( '(' );
		
		comma = false;
		for( String arg : argValue ) {
			
			if( comma )
				buf.append( ',' );
			
			comma = true;
			
			buf.append( arg );
		}
		
		buf.append( ")\n" );
		
		return buf.toString();
	}

	@Override
	public String defFunction( String funName, String outputName,
			String[] inputNameList, String body ) {
		
		StringBuffer buf;
		boolean comma;
		
		if( funName == null )
			throw new NullPointerException( "Function name must not be null." );
		
		if( funName.isEmpty() )
			throw new RuntimeException( "Function name must not be empty." );
		
		if( inputNameList == null )
			throw new NullPointerException( "Input name list must not be null." );
		
		if( body == null )
			throw new NullPointerException( "Function body must not be null." );
		
		buf = new StringBuffer();
		
		buf.append( "def " ).append( funName ).append( '(' );
		
		comma = false;
		for( String inputName : inputNameList ) {
			
			if( comma )
				buf.append( ',' );
			
			comma = true;
			
			buf.append( inputName );
		}
		
		buf.append( "):\n    " );
		buf.append( body.replace( "\n", "\n    " ) );
		buf.append( '\n' );
		
		return buf.toString();
	}

	@Override
	public String defFunctionLog() throws NotDerivableException {
		return
			defFunction( FUN_LOG, null, new String[] { "key", "value" },
				"f = file(\""+REPORT_FILENAME+"\",\"a\")\nf.write("+"\"{"
					+JsonReportEntry.ATT_TIMESTAMP+":%d,"
					+JsonReportEntry.ATT_RUNID+":\\\""+getDagId()+"\\\","
					+JsonReportEntry.ATT_TASKID+":"+getTaskNodeId()+","
					+JsonReportEntry.ATT_TASKNAME+":\\\""+getTaskName()+"\\\","
					+JsonReportEntry.ATT_LANG+":\\\""+getLangLabel()+"\\\","
					+JsonReportEntry.ATT_INVOCID+":"+getSignature()+","
					+JsonReportEntry.ATT_KEY+":\\\"%s\\\","
					+JsonReportEntry.ATT_VALUE+":%s}\\n\"%(time.time()*1000,key,value))\nf.close()\n" );
	}

	@Override
	public String getCheckPost() {
		return "";
	}

	@Override
	public String getShebang() {
		return "#!/usr/bin/env python\n";
	}

	@Override
	public String varDef( String varname, DataList list )
	throws NotDerivableException {
		
		StringBuffer buf;
		boolean comma;
		int i, n;
		
		buf = new StringBuffer();
		
		buf.append( varname ).append( "=[" );
		
		comma = false;
		n = list.size();
		for( i = 0; i < n; i++ ) {
			
			if( comma )
				buf.append( ',' );
			
			comma = true;
			
			buf.append( quote( list.get( i ).getValue() ) );
		}
		
		buf.append( "]\n" );
		
		return buf.toString();
	}

	@Override
	public String varDef( String varname, String value ) {
		return varname+"="+value+"\n";
	}

	@Override
	public String newList( String listName ) {
		return listName+"=[]\n";
	}

	@Override
	public String listAppend( String listName, String element ) {
		return listName+".append("+element+")\n";
	}

	@Override
	public String dereference( String varName ) {
		return varName;
	}

	@Override
	public String forEach( String listName, String elementName, String body ) {
		return
			"for "+elementName+" in "+listName+":\n    "
			+body.replace( "\n", "\n    " )+"\n";
	}

	@Override
	public String ifNotFileExists( String fileName, String body ) {
		return
			"if not(os.path.exists("+fileName+")):\n    "
			+body.replace( "\n", "\n    " )+"\n";
	}

	@Override
	public String raise( String msg ) {
		return "raise Exception("+quote( msg )+")";
	}

	@Override
	public String join( String... elementList ) {
		
		boolean comma;
		StringBuffer buf;
		
		buf = new StringBuffer();
		
		comma = false;
		for( String element : elementList ) {
			
			if( comma )
				buf.append( '+' );
			
			comma = true;
			
			buf.append( element );
		}
		
		return buf.toString();
	}

	@Override
	public String quote( String content ) {
		return "\""+content.replace( "\\", "\\\\" ).replace( "\"", "\\\"")+"\"";
	}

	@Override
	public String fileSize( String filename ) {
		return "os.stat("+filename+").st_size";
	}

	@Override
	public String ifListIsNotEmpty( String listName, String body ) {
		return "if not("+listName+"):\n    "+body.replace( "\n", "\n    " )+"\n";
	}

	@Override
	public String listToBraceCommaSeparatedString( String listName,
			String stringName, String open, String close ) {
		return
			stringName+"='"+open+"'+','.join( i for i in "+listName
			+" )+'"+close+"'\n";
	}

	@Override
	public String defFunctionNormalize() throws NotDerivableException {
		return defFunction(
			FUN_NORMALIZE,
			null,
			new String[] { "channel", "f" },
			"return '"+getSignature()+"_%s_%s'%(channel,os.path.basename(f))" );
	}

	@Override
	public String symlink( String src, String dest ) {
		return "os.symlink("+src+","+"dest"+")\n";
	}

	@Override
	public String getImport() {
		return "import os,time\n";
	}

	@Override
	public String comment( String comment ) {
		return "# "+comment.replace( "\n", "\n# " )+"\n";
	}

	@Override
	public String copyArray( String from, String to ) {
		return to+"="+from+"\n";
	}

	@Override
	public int getLangId() {
		return ForeignLangCatalog.LANGID_PYTHON;
	}
}
