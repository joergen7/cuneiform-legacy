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

public class RInvocation extends Invocation {

	protected RInvocation( TaskNode taskNode ) throws NotDerivableException {
		super( taskNode );
	}

	@Override
	public String getCheckPost() {
		return "";
	}

	@Override
	public String defFunctionLog() throws NotDerivableException {
		
		StringBuffer buf;
		
		buf = new StringBuffer();
		
		buf.append( defFunction( FUN_LOG, null, new String[] { "key", "value" },
			"if( mode( key ) != \"character\" )\n"
			+"stop( \"Expected key to be of type 'character'.\" )\n"
			+"if( mode( value ) != \"character\" )\n"
			+"stop( \"Expected key to be of type 'value'.\" )\n"
			+"write( paste( \"{"
			+JsonReportEntry.ATT_TIMESTAMP+":\", round( 1000*unclass( Sys.time() ) ), \","
			+JsonReportEntry.ATT_RUNID+":\\\""+getDagId()+"\\\","
			+JsonReportEntry.ATT_TASKID+":"+getTaskNodeId()+","
			+JsonReportEntry.ATT_TASKNAME+":"+getTaskName()+","
			+JsonReportEntry.ATT_LANG+":"+getLangLabel()+","
			+JsonReportEntry.ATT_INVOCID+":"+getSignature()+","
			+JsonReportEntry.ATT_KEY+":\\\"\", key, \"\\\","
			+JsonReportEntry.ATT_VALUE+":\\\"\", value, \"\\\"}\\n\" ), "
			+"file=\""+REPORT_FILENAME+"\" )" ) );
		
		return buf.toString();
	}

	@Override
	public String getShebang() {
		return "#!/usr/bin/env Rscript\n";
	}

	@Override
	public String varDef( String varname, DataList list )
			throws NotDerivableException {
		
		StringBuffer buf;
		int i;
		boolean comma;
		
		buf = new StringBuffer();
		
		comma = false;
		buf.append( varname ).append( " <- c( " );
		for( i = 0; i < list.size(); i++ ) {
			
			if( comma )
				buf.append( ',' );
			comma = true;
			
			buf.append( "\"" ).append( list.get( i ).getValue() ).append( "\"" );
		}
		
		buf.append( ")\n" );
		
		return buf.toString();
	}

	@Override
	public String varDef( String varname, String value ) {
		return varname+" <- "+value+"\n";
	}
	
	@Override
	public String defFunction( String funName, String outputName, String[] inputNameList, String body ) {
		
		StringBuffer buf;
		boolean comma;
		
		buf = new StringBuffer();
		
		buf.append( funName ).append( " <- function(" );
		
		comma = false;
		for( String arg : inputNameList ) {
			
			if( comma )
				buf.append( ',' );
			
			comma = true;
			
			buf.append( ' ' ).append( arg );
		}
		
		buf.append( " ) {\n" ).append( body ).append( "\n}\n" );
		
		return buf.toString();
	}

	@Override
	public String callFunction( String name, String... argValue ) {
		
		StringBuffer buf;
		boolean comma;
		
		buf = new StringBuffer();
		
		buf.append( name ).append( "( " );
		comma = false;
		for( String arg : argValue ) {

			if( comma )
				buf.append( ", " );
			comma = true;
			
			buf.append( arg );
		}
		
		buf.append( " )" );
		
		return buf.toString();
	}

	@Override
	public String newList( String listName ) {
		return varDef( listName, "NULL" );
	}

	@Override
	public String listAppend( String listName, String element ) {
		return listName+" <- append( "+listName+", "+element+" )\n";
	}

	@Override
	public String dereference( String varName ) {
		return varName;
	}

	@Override
	public String forEach( String listName, String elementName, String body ) {
		return "for( "+elementName+" in "+listName+" ) {\n"+body+"\n}\n";
	}

	@Override
	public String ifNotFileExists( String fileName, String body ) {
		return
			"if( !"
			+callFunction( "file.exists", dereference( fileName ) )
			+" ) {\n"
			+body
			+"\n}\n";
	}

	@Override
	public String raise( String msg ) {
		return callFunction( "stop", msg );
	}

	@Override
	public String join( String... elementList ) {
		
		StringBuffer buf;
		boolean comma;
		
		buf = new StringBuffer();
		
		buf.append( "paste( " );
		
		comma = false;
		for( String element : elementList ) {
			
			if( comma )
				buf.append( ", " );
			comma = true;
			
			buf.append( element );
		}
		
		buf.append( ", sep=\"\" )" );
		
		return buf.toString();
	}

	@Override
	public String quote( String content ) {
		return "\""+content.replace( "\"", "\\\"" )+"\"";
	}

	@Override
	public String fileSize( String filename ) {
		return "file.info( "+quote( filename )+" )$size";
	}

	@Override
	public String ifListIsNotEmpty( String listName, String body ) {
		return "if( length( "+listName+" ) != 0 ) {\n"+body+"\n}\n";
	}

	@Override
	public String listToBraceCommaSeparatedString( String listName,
		String stringName, String open, String close ) {
		return stringName+" = paste( "+listName+", sep=\"\", collapse=\",\" )\n";
	}

	@Override
	public String defFunctionNormalize() throws NotDerivableException {
		return defFunction(
				FUN_NORMALIZE,
				null,
				new String[] { "channel", "f" },
				"sprintf( \""+getSignature()
				+"_%d_%s\", channel, f )\n" );
	}

	@Override
	public String symlink( String src, String dest ) {
		return callFunction( "file.symlink", src, dest )+"\n";
	}

	@Override
	public String getImport() {
		return "";
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
		return ForeignLangCatalog.LANGID_R;
	}

}
