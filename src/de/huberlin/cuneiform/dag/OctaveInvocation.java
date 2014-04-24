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

public class OctaveInvocation extends Invocation {

	public OctaveInvocation( TaskNode taskNode ) throws NotDerivableException {
		super( taskNode );
	}

	@Override
	public String getShebang() {
		return "#!/usr/bin/octave -q\n";
	}

	@Override
	public String varDef( String varname, DataList list )
	throws NotDerivableException {
		
		StringBuffer buf;
		int i;
		boolean comma;
		
		buf = new StringBuffer();
		
		buf.append( varname )
			.append( " = {" );
		
		comma = false;
		for( i = 0; i < list.size(); i++ ) {
			
			if( comma )
				buf.append( ';' );
			
			comma = true;
			
			buf.append( " '" ).append( list.get( i ).getValue() ).append( "'" );
		}
		
		buf.append( "};\n" );
		
		return buf.toString();
	}

	@Override
	public String varDef( String varname, String value ) {		
		return varname+" = '"+value+"';\n";
	}

	@Override
	public String getCheckPost() {
		return "";
	}
	
	@Override
	public String defFunction( String funName, String outputName, String[] inputNameList, String body ) {
		
		String ret;
		boolean comma;
		
		ret = "function ";
		
		if( outputName != null )
		 ret += outputName+" = ";
		
		ret += funName+"( ";
		
		comma = false;
		for( String inputName : inputNameList ) {
			
			if( comma )
				ret += ", ";
			
			comma = true;
			
			ret += inputName;
		}
		
		ret += " )\n"+body+"\nend\n";
		
		return ret;
	}
	
	private static String matlabForLoop( String runVar, String times, String body ) {
		return "for "+runVar+" = 1:"+times+"\n"+body+"\nend\n";
	}
	
	@Override
	public String callFunction( String name, String... argValue ) {
		
		StringBuffer buf;
		boolean comma;
		
		buf = new StringBuffer();
		
		buf.append( name ).append( "( " );
		
		comma = false;
		for( String value : argValue ) {
			
			if( comma )
				buf.append( ", " );
			
			comma = true;
			
			buf.append( value );
		}
		
		buf.append( " )" );
		
		return buf.toString();
			
	}

	@Override
	public String defFunctionLog() throws NotDerivableException {
		return defFunction(
				FUN_LOG, null,
				new String[] { "key", "value" },
				"if( ~ischar( key ) ); error( 'Parameter ''key'' must be of type char.' ); end\n"
				+"if( ~ischar( value ) ); error( 'Parameter ''value'' must be of type char.' ); end\n"
				+"[fid msg] = fopen( '"+REPORT_FILENAME+"', 'a' );\n"
				+"if( fid < 0 ); error( msg ); end\n"
				+"fprintf( fid, '{"
				+JsonReportEntry.ATT_TIMESTAMP+":%d000,"
				+JsonReportEntry.ATT_RUNID+":\""+getDagId()+"\","
				+JsonReportEntry.ATT_TASKID+":"+getTaskNodeId()+","
				+JsonReportEntry.ATT_INVOCID+":"+getSignature()+","
				+JsonReportEntry.ATT_TASKNAME+":\""+getTaskName()+"\","
				+JsonReportEntry.ATT_LANG+":\""+getLangLabel()+"\","
				+JsonReportEntry.ATT_KEY+":\"%s\","
				+JsonReportEntry.ATT_VALUE+":%s}\\n', time(), key, value )\n"
				+"if( fclose( fid ) < 0 ); error( 'Could not close report file' ); end" );
	}

	@Override
	public String newList( String listName ) {
		return listName+" = {};\n";
	}

	@Override
	public String listAppend( String listName, String element ) {
		return listName+" = ["+listName+" {"+element+"}];";
	}

	@Override
	public String dereference( String varName ) {
		return varName;
	}

	@Override
	public String forEach( String listName, String elementName, String body ) {
		
		StringBuffer buf;
		
		buf = new StringBuffer();
		
		buf.append( "for i = 1:length( " ).append( listName ).append( ")\n" );
		buf.append( varDef( elementName, listName+"{ i };\n" ) );
		buf.append( body );
		buf.append( "\nend\n" );
		
		return buf.toString();
	}

	@Override
	public String ifNotFileExists( String fileName, String body ) {
		return "if exist( "+fileName+", 'file' ) == 2\n"+body+"\nend\n";
	}

	@Override
	public String raise( String msg ) {
		return callFunction( "error", quote( msg ) );
	}

	@Override
	public String join( String... elementList ) {
		
		StringBuffer buf;
		boolean comma;
		
		buf = new StringBuffer();
		
		buf.append( "[" );
		
		comma = false;
		for( String element : elementList ) {
			
			if( comma )
				buf.append( " " );
			comma = true;
			
			buf.append( element );
		}
		buf.append( "]" );
		
		return buf.toString();
	}

	@Override
	public String quote( String content ) {
		return "\""+content+"\"";
	}

	@Override
	public String fileSize( String filename ) {
		return "stat( "+filename+" ).size";
	}

	@Override
	public String ifListIsNotEmpty( String listName, String body ) {
		return "if ~isempty( "+listName+" )\n"+body+"\nend\n";
	}

	@Override
	public String listToBraceCommaSeparatedString( String listName,
			String stringName, String open, String close ) {

		StringBuffer buf;
		
		buf = new StringBuffer();
		
		buf.append( stringName ).append( " = '" ).append( open );
		buf.append( "';\n__COMMA = false;\n" );
		buf.append( 
			matlabForLoop( "i", "length( "+listName+" )",
				"if( __COMMA ); ret = [ret ',']; end\n__COMMA = true;\n"
				+"ret = [ret "+listName+"{ i }];\n" ) );
		buf.append( "ret = [ret '" ).append( close ).append( "'];" );
		
		return buf.toString();
	}

	@Override
	public String defFunctionNormalize() throws NotDerivableException {
		return defFunction(
			FUN_NORMALIZE,
			"norm",
			new String[] {"channel", "f"},
			"[x name] = fileparts( f );\n"
			+"norm = ['"+getSignature()+"_' channel '_' name];\n" );
	}

	@Override
	public String symlink( String src, String dest ) {
		return
			"system( sprintf( 'ln -s %s %s', "+src+", "+dest+" ) )"; 
	}

	@Override
	public String getImport() {
		return "";
	}

	@Override
	public String comment( String comment ) {
		return "% "+comment.replace( "\n", "\n# " )+"\n";
	}

	@Override
	public String copyArray( String from, String to ) {
		return to+"="+from+";\n";
	}

	@Override
	public int getLangId() {
		return ForeignLangCatalog.LANGID_OCTAVE;
	}

}
