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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.cuneiform.common.Constant;
import de.huberlin.cuneiform.common.ForeignLangCatalog;
import de.huberlin.cuneiform.language.DefTask;


public abstract class Invocation implements Computable {
	
	private static int runningId;

		
	public static final String REPORT_FILENAME = "__report__.txt";
	private static final long PRIME = Long.valueOf( "99194853094755497" ); // large prime number below 9223372036854775807
	
	private static final String[] STATE_LABEL = { "Enumerable", "Ready", "Success", "Failed" };
	
	public static final int STATEID_ENUMERABLE = 0;
	public static final int STATEID_READY = 1;
	public static final int STATEID_SUCCESS = 2;
	public static final int STATEID_FAILED = 3;

	
	public static final String FUN_LOG = "cflogmsg";
	public static final String FUN_NORMALIZE = "cfnormalize";
	public static final String FUN_USERLOG = "logmsg";


	private int id;
	private List<Integer> sizeList;
	private List<DataList> dataListList;
	
	private Map<String,Resolveable> singleParamMap;
	private Map<String,DataList> reduceParamMap;
	private TaskNode taskNode;
	
	protected Invocation( TaskNode taskNode ) throws NotDerivableException {
		
		int i;
		
		sizeList = new LinkedList<>();
		dataListList = new LinkedList<>();
		
		setTaskNode( taskNode );
		
		for( i = 0; i < nOutputChannel(); i++ ) {
			
			sizeList.add( null );
			dataListList.add( null );
		}
		
		singleParamMap = new HashMap<>();
		reduceParamMap = new HashMap<>();
		
		setId();
		
	}
	
	public void bindOutput( String outputName, JSONArray array ) throws NotDerivableException, JSONException {
		
		DataList dataList;
		int i;
		
		if( outputName == null )
			throw new NullPointerException( "Output name must not be null." );
		
		if( outputName.isEmpty() )
			throw new RuntimeException( "Output name must not be empty." );
		
		if( array == null )
			throw new NullPointerException( "JSON array must not be null." );
		
		dataList = new DataList();
		
		for( i = 0; i < array.length(); i++ )
			dataList.add( new DataItem( array.getString( i ) ) );
		
		i = getDefTask().outputIndexOf( outputName );
		
		dataListList.set( i, dataList );
		try {
			sizeList.set( i, dataList.size() );
		}
		catch( NotDerivableException e ) {
			throw new RuntimeException(
				"Trying to bind data list of unknown size." );
		}
	}
	
	public void bindParam( String paramName, Resolveable content ) {
		
		if( content == null )
			throw new NullPointerException(
				"Data list content must not be null." );
		
		if( paramName == null )
			throw new NullPointerException(
				"Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		singleParamMap.put( paramName, content );
	}
	
	public void bindParam( String paramName, DataList content ) {
		
		if( content == null )
			throw new NullPointerException(
				"Data list content must not be null." );
		
		if( paramName == null )
			throw new NullPointerException(
				"Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		reduceParamMap.put( paramName, content );
	}
	
	public void evalReport( Set<JsonReportEntry> report ) throws JSONException, NotDerivableException {

		JSONObject payload;
		

		for( JsonReportEntry entry : report )
						
			try {

				if( entry.getKey().equals( JsonReportEntry.KEY_INVOC_OUTPUT ) ) {
					
					payload = entry.getValueJsonObj();
					
					for( String outputName : getOutputNameList() )
					
						bindOutput( outputName, payload.getJSONArray( outputName ) );
					
				}
			}
			catch( JSONException e ) {
				System.err.println( "[entry]" );
				System.err.println( entry );
				System.err.println( "[value]" );
				System.err.println( entry.getValue() );
				
				throw e;
			}
		
	}
	
	public String getBody() throws NotDerivableException {
		return getDefTask().getBody();
	}
	
	public String getDagId() {
		return taskNode.getDagId();
	}

	public Resolveable getResolveable( int outputChannel, int idx ) {
		
		if( idx < 0 )
			throw new IndexOutOfBoundsException(
				"Index must not be smaller than 0." );
		
		return new ResolvedInvocationReference( this, outputChannel, idx );		
	}

	public DataList getDataList( int outputChannel )
	throws NotDerivableException {
		
		int n;
		DataList list;
		
		if( outputChannel < 0 )
			throw new IndexOutOfBoundsException(
				"Output channel must not be smaller than 0." );
		
		list = dataListList.get( outputChannel );
		
		if( list == null )
			throw new NotDerivableException(
				"Data list for output channel "+outputChannel
				+" not bound for invocation signature="
				+getSignature()+"." );
		
		
		n = size( outputChannel );
		
		if( n != list.size() )
			throw new RuntimeException(
				"Concrete size does not match inferred size." );
		
		
		return list;
	}
	
	public DefTask getDefTask() throws NotDerivableException {
		return taskNode.getDag().getDefTask( getTaskName() );
	}
	
	public int getId() {
		return id;
	}
	
	public List<String> getOutputNameList() throws NotDerivableException {
		return getDefTask().getOutputNameList();
	}
	
	public abstract int getLangId();
	
	public String getLangLabel() {
		return ForeignLangCatalog.langIdToLabel( getLangId() );
	}
	
	public DataList getDataListBoundToParam( String paramName ) {
		
		DataList dataList;
		
		if( paramName == null )
			throw new NullPointerException(
				"Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		dataList = new DataList();
		
		if( singleParamMap.containsKey( paramName ) ) {
			
			dataList.add( singleParamMap.get( paramName ) );
			
			return dataList;
		}
		
		if( reduceParamMap.containsKey( paramName ) ) {
			
			dataList.add( reduceParamMap.get( paramName ) );
			
			return dataList;
		}
		
		throw new RuntimeException(
			"Parameter with the name '"+paramName+"' not bound." );
	}
	
	public Set<String> getParamNameSet() {
		
		Set<String> set;
		
		set = new HashSet<>();
		
		set.addAll( singleParamMap.keySet() );
		set.addAll( reduceParamMap.keySet() );
		
		return set;
	}
	
	public Set<String> getReduceOutputNameSet() throws NotDerivableException {
		return getDefTask().getReduceOutputNameSet();
	}
	
	public Set<String> getReduceParamNameSet() {
		return reduceParamMap.keySet();
	}
	
	public Set<String> getSingleOutputNameSet() throws NotDerivableException {
		return getDefTask().getSingleOutputNameSet();
	}
	
	public Set<String> getSingleParamNameSet() {
		return singleParamMap.keySet();
	}
	
	public Resolveable getResolveableBoundToSingleParam( String paramName ) {
		
		Resolveable item;
		
		if( paramName == null )
			throw new NullPointerException(
				"Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		item = singleParamMap.get( paramName );
		
		if( item == null )
			throw new NullPointerException(
				"The parameter with the name '"+paramName
				+"' has not been bound." );
		
		return item;
	}
	
	public int getStateId() {
		
		if( isComputed() )
			return STATEID_SUCCESS;
		
		if( isReady() )
			return STATEID_READY;
		
		return STATEID_ENUMERABLE;
	}
	
	public String getStateLabel() {
		return STATE_LABEL[ getStateId() ];
	}
	
	public int getOutputChannel( String outputName ) throws NotDerivableException {
		return getDefTask().getOutputChannel( outputName );
	}
	
	public String getOutputName( int outputChannel ) throws NotDerivableException {
		return getDefTask().getOutputName( outputChannel );
	}
	
	public int getOutputType( String outputName ) throws NotDerivableException {
		return getDefTask().getOutputType( outputName );
	}
	
	public Set<Invocation> getParentInvocationSet() {
		
		Set<Invocation> set;
		
		set = new HashSet<>();
		
		for( Resolveable res : singleParamMap.values() )
			if( res.isInvocation() )
				set.add( res.getInvocation() );
		
		for( DataList dataList : reduceParamMap.values() )
			set.addAll( dataList.getInvocationSet() );
		
		return set;
	}
	
	public DataList getReduceParam( String paramName ) {
		
		DataList list;
		
		if( paramName == null )
			throw new NullPointerException(
				"Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		list = reduceParamMap.get( paramName );
		
		if( list == null )
			throw new NullPointerException(
				"The parameter with the name '"+paramName
				+"' has not been bound." );
		
		return list;
		
	}
	
	public List<String> getStageInList() throws NotDerivableException {
		
		List<String> list;
		DataList l;
		Resolveable c;
		int i;
		
		list = new LinkedList<>();
		
		for( String paramName : singleParamMap.keySet() )			
			if( this.isParamStage( paramName ) ) {
				
				c = singleParamMap.get( paramName );
				if( c == null )
					throw new NotDerivableException( "Data item has no realization." );
				
				list.add( c.getValue() );
			}
		
		for( String paramName : reduceParamMap.keySet() )			
			if( this.isParamStage( paramName ) ) {
				
				l = reduceParamMap.get( paramName );
				if( l == null )
					throw new NotDerivableException( "Data item has no realization." );
				
				for( i = 0; i < l.size(); i++ )
					list.add( l.get( i ).getValue() );
			}
		
		
		return list;
	}
	
	public List<String> getStageOutList() throws NotDerivableException {
		
		List<String> list;
		int i, j;
		DataList cur;
		
		list = new LinkedList<>();
		
		for( i = 0; i < taskNode.nOutputChannel(); i++ ) {
			
			cur = dataListList.get( i );
			
			if( cur == null )
				throw new NotDerivableException(
					"Output channel "+i+" has never been bound." );
			
			if( isOutputStage( i ) )
				for( j = 0; j < cur.size(); j++ )
					list.add( cur.get( j ).getValue() );
		}
		
		return list;
	}
	
	public String getTaskName() throws NotDerivableException {
		return singleParamMap.get( Constant.TOKEN_TASK ).getValue();
	}
	
	public TaskNode getTaskNode() {
		return taskNode;
	}
	
	public int getTaskNodeId() {
		return taskNode.getId();
	}
	
	public String getWfName() {
		return taskNode.getWfName();
	}
	
	public long getSignature() throws NotDerivableException {
		
		long hash;
		int i;
		DataList list;
		
		hash = add( add( 0, getTaskName() ), getBody() );
		
		/* for( String paramName : getParamNameSet() ) {
			
			if( isParamStage( paramName ) )
				hash++;
			
			if( isParamReduce( paramName ) )
				hash += 2;
			
			hash = add( hash, paramName );
		} */
		
		for( String outputName : getOutputNameList() ) {
			
			if( isOutputStage( outputName ) )
				hash += 4;
			
			if( isOutputReduce( outputName ) )
				hash += 8;
			
			hash = add( hash, outputName );
		}
		
		for( String key : singleParamMap.keySet() )
			hash = add( hash, key+singleParamMap.get( key ).getValue() );
		
		for( String key : reduceParamMap.keySet() ) {
			list = reduceParamMap.get( key );
			for( i = 0; i < list.size(); i++ )
				hash = add( hash, key+i+list.get( i ).getValue() );
		}
			
		
		/* for( Resolveable item : singleParamMap.values() )
			hash = add( hash, item.getValue() );
		
		for( DataList list : reduceParamMap.values() )
			for( i = 0; i < list.size(); i++ )
				hash = add( hash, list.get( i ).getValue() ); */
		
		
		return hash;
	}
	
	private static long add( long a, Object b ) {
		
		long hash;
		
		hash = a;
		hash = ( hash+( abs( b.hashCode() )%PRIME ) )%PRIME;
		
		return hash;
	}
	
	@Override
	public boolean isComputed() {
		
		for( DataList dataList : dataListList )
			if( dataList == null )
				return false;
		
		return true;
	}
	
	public boolean isReady() {
		
		if( isComputed() )
			return false;
		
		try {
			getSignature();
			return true;
		}
		catch( NotDerivableException e ) {
			return false;
		}
	}
	
	public boolean isOutputStage( String outputName ) throws NotDerivableException {
		return getDefTask().isOutputStage( outputName );
	}
	
	public boolean isOutputReduce( String outputName ) throws NotDerivableException {
		return getDefTask().isOutputReduce( outputName );
	}
	
	public boolean isOutputStage( int outputChannel ) throws NotDerivableException {
		return getDefTask().isOutputStage( outputChannel );
	}
	
	public boolean isParamReduce( String paramName ) throws NotDerivableException {
		return getDefTask().isParamReduce( paramName );
	}
	
	public boolean isParamStage( String paramName ) throws NotDerivableException {
		return getDefTask().isParamStage( paramName );
	}
	
	public int nOutputChannel() throws NotDerivableException {
		return taskNode.nOutputChannel();
	}
	
	public void setSize( int outputChannel, Integer size ) {
		
		if( size == null ) {
			sizeList.set( outputChannel, null );
			return;
		}
		
		if( size < 0 )
			throw new RuntimeException(
				"Invocation size must not be less than 0." );
		
		sizeList.set( outputChannel, size );
	}
	
	public void setTaskNode( TaskNode taskNode ) {
		
		if( taskNode == null )
			throw new NullPointerException( "Task node must not be null." );
		
		this.taskNode = taskNode;
	}
	
	public int size( int outputChannel ) throws NotDerivableException {
		
		Integer n;
		
		n = sizeList.get( outputChannel );

		if( n == null )
			throw new NotDerivableException( "No size information given." );
		
		return n;
	}
	
	public String toScript() throws NotDerivableException {
		
		StringBuffer buf;
		
		buf = new StringBuffer();
		
		// insert shebang
		buf.append( getShebang() ).append( '\n' );
		
		// import libraries
		buf.append( comment( "import libraries" ) );
		buf.append( getImport() ).append( '\n' );
		
		// define necessary functions
		buf.append( comment( "define necessary functions" ) );
		buf.append( getFunDef() ).append( '\n' );

		// bind single output variables to default values
		buf.append( comment( "bind single output variables to default values" ) );
		for( String outputName : getSingleOutputNameSet() )
			buf.append(
				varDef(
					outputName,	
					quote( outputName ) ) );
		buf.append( '\n' );
		
		// bind input parameters
		buf.append( comment( "bind input parameters" ) );
		for( String paramName : getSingleParamNameSet() ) {
			
			if( paramName.equals( Constant.TOKEN_TASK ) )
				continue;
			
			buf.append( varDef( paramName, getResolveableBoundToSingleParam( paramName ).getValue() ) );
		}
		for( String paramName : getReduceParamNameSet() )
			buf.append( varDef( paramName, getReduceParam( paramName ) ) );		
		buf.append( '\n' );
		
		// report stage in file sizes and report error when something is missing
		buf.append( comment( "report stage in file sizes and report error when something is missing" ) );
		buf.append( getStageInCollect() ).append( '\n' );
				
		// insert function body
		buf.append( comment( "insert function body" ) );
		buf.append( getBody() ).append( '\n' );
		
		// check post
		buf.append( comment( "check post" ) );
		buf.append( getCheckPost() ).append( '\n' );
		
		// rename output files
		buf.append( comment( "rename output files" ) );
		buf.append( getOutputRename() ).append( '\n' );
		
		// collect output variables
		buf.append( comment( "collect output variables" ) );
		buf.append( getOutputCollect() ).append( '\n' );
		
		// collect stage out information
		buf.append( comment( "collect stage out information" ) );
		buf.append( getStageOutCollect() ).append( '\n' );
		
		return buf.toString();
	}

	@Override
	public String toString() {
		
		String ret;
		int i, s;
		boolean comma;
		
		ret = "Invocation ";
		
		try {

			ret = this.getTaskName()+"( ";
		
		
			for( String paramName : singleParamMap.keySet() )
				ret += paramName+" : "+singleParamMap.get( paramName )+" ";
			
			for( String paramName : reduceParamMap.keySet() )
				ret += paramName+" : "+reduceParamMap.get( paramName )+" ";
			
			ret += ")->[";
			
			comma = false;
			for( i = 0; i < nOutputChannel(); i++ ) {
				
				if( comma )
					ret += ", ";
				
				comma = true;
				
				try {
					s = size( i );
					ret += s;
				}
				catch( NotDerivableException e ) {
					ret += "?";
				}
				
			}
				
			ret += "]";
			
		}
		catch( NotDerivableException e ) {
			// just return what you have
		}
		return ret;
	}

	public String varDef( String varname, Resolveable item ) throws NotDerivableException {
		return varDef( varname, item.getValue() );
	}
	
	public static Invocation createInvocation(
		TaskNode taskNode, String taskName )
	throws NotDerivableException {
		
		int langId, j;
		Invocation invoc;
		DefTask defTaskExample;
				
		langId = taskNode.getDag().getDefTask( taskName ).getLangLabelId();
		
		switch( langId ) {
		
			case ForeignLangCatalog.LANGID_BASH :
				invoc = new BashInvocation( taskNode ); break;
				
			case ForeignLangCatalog.LANGID_LISP :
				invoc = new LispInvocation( taskNode ); break;
				
			case ForeignLangCatalog.LANGID_OCTAVE :
				invoc = new OctaveInvocation( taskNode ); break;
				
			case ForeignLangCatalog.LANGID_PERL :
				invoc = new PerlInvocation( taskNode ); break;
				
			case ForeignLangCatalog.LANGID_PYTHON :
				invoc = new PythonInvocation( taskNode ); break;
				
			case ForeignLangCatalog.LANGID_R :
				invoc = new RInvocation( taskNode ); break;
				
			case ForeignLangCatalog.LANGID_SCALA :
				invoc = new ScalaInvocation( taskNode ); break;
			
			default :
				throw new RuntimeException(
					"Unknown foreign language id "+langId+"." );
		}
		

		// set task name
		invoc.bindParam( Constant.TOKEN_TASK, new DataItem( taskName ) );

		defTaskExample = taskNode.getDefTaskExample();
		
		// set size information if possible
		for( j = 0; j < taskNode.nOutputChannel(); j++ )
			if( !defTaskExample.isOutputReduce( j ) )
				invoc.setSize( j, 1 );

		return invoc;
	}

	private static int abs( int x ) {
		
		if( x < 0 )
			return -x;
		
		return x;
	}
	
	private synchronized void setId() {
		id = runningId++;
	}
	
	public String getStageInCollect() throws NotDerivableException {
		
		StringBuffer buf;
		boolean encounter;
		
		buf = new StringBuffer();
		
		encounter = false;
		
		// collect all staged input files
		buf.append( newList( "CFLIST" ) );
		for( String inputName : getSingleParamNameSet() )
			if( isParamStage( inputName ) ) {
				buf.append( listAppend( "CFLIST", dereference( inputName ) ) );
				encounter = true;
			}
		
		for( String inputName : getReduceParamNameSet() )
			if( isParamStage( inputName ) ) {
				buf.append( listAppend( "CFLIST", dereference( inputName ) ) );
				encounter = true;
			}

		// check whether the files exist and associate each file in the list with its size
		buf.append( newList( "CFLIST1" ) );
		buf.append(
			forEach(
				"CFLIST", "CFI",
				ifNotFileExists(
					dereference( "CFI" ),
					raise( join( quote( "Stage in: A file " ), dereference( "CFI" ),
						quote( " should be present but has not been found." ) ) ) )+
				listAppend( "CFLIST1", join( quote( "\"" ), dereference( "CFI" ), quote( "\":" ), fileSize( dereference( "CFI" ) ) ) ) ) );
		
		// if the file list to be staged in is not empty, write it to the log
		buf.append(
			ifListIsNotEmpty(
				"CFLIST1",
				listToBraceCommaSeparatedString(
					"CFLIST1",
					"CFPAYLOAD", "{", "}" )
				+callFunction( FUN_LOG, quote( JsonReportEntry.KEY_FILE_SIZE_STAGEIN ), dereference( "CFPAYLOAD" ) ) ) );

		if( encounter )
			return buf.toString();
		
		return "";
	}
	
	public String getOutputCollect() throws NotDerivableException {
		
		StringBuffer buf;
		
		buf = new StringBuffer();
		
		buf.append( newList( "CFLIST" ) );
		
		for( String outputName : getSingleOutputNameSet() ) {
			
			buf.append(
				listAppend(
					"CFLIST",
					join(
						quote( outputName+":[\"" ),
						dereference( outputName ),
						quote( "\"]" ) ) ) );
		}
		
		for( String outputName : getReduceOutputNameSet() ) {
			
			buf.append( newList( "CFLIST1" ) );
			
			buf.append(
				forEach(
					outputName,
					"CFI",
					listAppend(
						"CFLIST1",
						join(
							quote( "\"" ),
							dereference( "CFI" ),
							quote( "\"" ) ) ) ) );
			
			buf.append(
				listToBraceCommaSeparatedString(
					"CFLIST1", "CFSTR", "[", "]" ) );

			buf.append(
				listAppend(
					"CFLIST",
					join(
						quote( outputName+":" ),
						dereference( "CFSTR" )
					) ) );
		}
		
		buf.append(
			listToBraceCommaSeparatedString( "CFLIST", "CFSTR", "{", "}" ) );
		
		buf.append(
			callFunction(
				FUN_LOG,
				quote( JsonReportEntry.KEY_INVOC_OUTPUT ),
				dereference( "CFSTR" ) ) ).append( '\n' );
		
		return buf.toString();
		
	}
	
	public String getFunDef() throws NotDerivableException {
		return defFunctionLog()
			// +defFunctionLogUsr()
			+defFunctionNormalize();
	}
	
	public String getOutputRename() throws NotDerivableException {
		
		StringBuffer buf;
		
		buf = new StringBuffer();

		for( String outputName : getSingleOutputNameSet() )
			
			if( isOutputStage( outputName ) ) {
				
				buf.append(
					varDef(
						"CFFILENAME",
						callFunction(
							FUN_NORMALIZE,
							String.valueOf( getOutputChannel( outputName ) ),
							dereference( outputName ) ) ) );
				
				buf.append(
					symlink(
						dereference( outputName ),
						dereference( "CFFILENAME" ) ) );
							
				buf.append(
					varDef(
						outputName,
						dereference( "CFFILENAME" ) ) );
			}
		
		for( String outputName : getReduceOutputNameSet() )

			if( isOutputStage( outputName ) ) {
				
				buf.append( newList( "CFLIST" ) );
				
				buf.append(
					forEach(
						outputName,
						"CFFILENAME",
						
						varDef(
							"CFNEWFILENAME",
							callFunction(
								FUN_NORMALIZE,
								String.valueOf(
									getOutputChannel( outputName ) ),
								dereference( "CFFILENAME" ) ) )
								
						+listAppend( "CFLIST", dereference( "CFNEWFILENAME" ) )
						
						+symlink(
							dereference( "CFFILENAME" ),
							dereference( "CFNEWFILENAME" ) ) ) );

				// buf.append( varDef( outputName, dereference( "__LIST" ) ) );
				buf.append( copyArray( "CFLIST", outputName ) );
			}
		
		return buf.toString();
	}
	
	public String getStageOutCollect() throws NotDerivableException {
		
		StringBuffer buf;
		boolean encounter;
		
		buf = new StringBuffer();
		
		encounter = false;
		
		// collect all staged output files
		buf.append( newList( "CFLIST" ) );
		for( String outputName : getSingleOutputNameSet() )
			if( isOutputStage( outputName ) ) {
				buf.append( listAppend( "CFLIST", dereference( outputName ) ) );
				encounter = true;
			}
		
		for( String outputName : getReduceOutputNameSet() )
			if( isOutputStage( outputName ) ) {
				buf.append( listAppend( "CFLIST", dereference( outputName ) ) );
				encounter = true;
			}

		// check whether the files exist and associate each file in the list with its size
		buf.append( newList( "CFLIST1" ) );
		buf.append(
			forEach(
				"CFLIST", "CFI",
				ifNotFileExists(
					dereference( "CFI" ),
					raise( join( quote( "Stage out: A file " ), dereference( "CFI" ),
						quote( " should be present but has not been found." ) ) ) )+ 
				listAppend( "CFLIST1", join( quote( "\"" ), dereference( "CFI" ), quote( "\":" ), fileSize( dereference( "CFI" ) ) ) ) ) );
		
		// if the file list to be staged out is not empty, write it to the log
		buf.append(
			ifListIsNotEmpty(
				"CFLIST1",
				listToBraceCommaSeparatedString(
					"CFLIST1",
					"CFPAYLOAD", "{", "}" )
				+callFunction( FUN_LOG, quote( JsonReportEntry.KEY_FILE_SIZE_STAGEOUT ), dereference( "CFPAYLOAD" ) ) ) );

		if( encounter )
			return buf.toString();
		
		return "";
	}
	
	public String quote( int str ) {
		return quote( String.valueOf( str ) );
	}
	
	public String defFunctionLogUsr() {
		return defFunction(
			FUN_USERLOG,
			null,
			new String[] { "msg" },
			callFunction(
				FUN_LOG,
				quote( JsonReportEntry.KEY_INVOC_USER ),
				"\\\""+dereference( "msg" )+"\\\"" ) );
	}

	public abstract String callFunction( String name, String... argValue );
	public abstract String defFunction( String funName, String outputName, String[] inputNameList, String body );
	public abstract String defFunctionLog()throws NotDerivableException;
	public abstract String getCheckPost();
	public abstract String getImport();
	public abstract String getShebang();
	public abstract String varDef( String varname, DataList list ) throws NotDerivableException;
	public abstract String varDef( String varname, String value );
	public abstract String newList( String listName );
	public abstract String listAppend( String listName, String element );
	public abstract String dereference( String varName );
	public abstract String forEach( String listName, String elementName, String body );
	public abstract String ifNotFileExists( String fileName, String body );
	public abstract String raise( String msg );
	public abstract String join( String ... elementList );
	public abstract String quote( String content );
	public abstract String fileSize( String filename );
	public abstract String ifListIsNotEmpty( String listName, String body );
	public abstract String listToBraceCommaSeparatedString( String listName, String stringName, String open, String close );
	public abstract String defFunctionNormalize() throws NotDerivableException;
	public abstract String symlink( String src, String dest );
	public abstract String comment( String comment );
	public abstract String copyArray( String from, String to );
}
