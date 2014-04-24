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

@Deprecated
public class ReportEntry {
	
	private static final String DELIM_COLUMN = "|";
	
	private String dagId;
	private long invocationId;
	private String key;
	private String payload;
	private String taskName;
	private int taskNodeId;
	private long timeStamp;
	private String wfName;
	
	public ReportEntry(
		String dagId, String wfName, int taskNodeId, long invocationId,
		String taskName, String key, String payload, long timeStamp ) {
		
		setDagId( dagId );
		setTaskNodeId( taskNodeId );
		setInvocationId( invocationId );
		setTaskName( taskName );
		setKey( key );
		setPayload( payload );
		setTimeStamp( timeStamp );
		setWfName( wfName );
	}
	
	public ReportEntry(
		String dagId, String wfName, int taskNodeId, long invocationId,
		String taskName, String key, String payload ) {
		
		this(
			dagId, wfName, taskNodeId, invocationId, taskName, key, payload,
			System.currentTimeMillis() );
	}
	
	public ReportEntry( TaskNode taskNode, long invocationId, String taskName, String key, String payload ) {
		this( taskNode.getDagId(), taskNode.getWfName(), taskNode.getId(), invocationId, taskName, key, payload );
	}
	
	public ReportEntry( Invocation invocation, String key, String payload ) throws NotDerivableException {		
		this( invocation.getTaskNode(), invocation.getSignature(), invocation.getTaskName(), key, payload );
	}
	
	public ReportEntry( String line ) {
		
		String[] token;
		int i;
		
		
		token = line.trim().split( "\\|" );
		
		if( token.length < 8 )
			throw new RuntimeException(
				"Report entry not well formed. Expected 8 '|' delimited fields."
				+" Found "+token.length+": '"+line+"'." );

		timeStamp = Long.valueOf( token[ 0 ] );
		setDagId( token[ 1 ] );
		wfName = token[ 2 ];
		taskNodeId = Integer.valueOf( token[ 3 ] );
		invocationId = Long.valueOf( token[ 4 ] );
		taskName = token[ 5 ];
		key = token[ 6 ];
		payload = token[ 7 ];
		
		for( i = 8; i < token.length; i++ )
			payload += "|"+token[ i ];
		
	}
	
	public String getDagId() {
		return dagId;
	}
	
	public long getInvocationSignature() {
		return invocationId;
	}
	
	public String getKey() {
		return key.replace( "\\n", "\n" );
	}
	
	public String getPayload() {
		return payload.replace( "\\n", "\n" );
	}
	
	public String getTaskName() {
		return taskName;
	}
	
	public int getTaskNodeSignature() {
		return taskNodeId;
	}
	
	public long getTimeStamp() {
		return timeStamp;
	}
	
	public String getWfName() {
		return wfName;
	}
	
	public void setDagId( String dagId ) {
		
		if( dagId == null )
			throw new NullPointerException( "Dag id must not be null." );
		
		if( dagId.isEmpty() )
			throw new RuntimeException( "Dag id must not be empty." );
		
		this.dagId = dagId;
	}
	
	public void setInvocationId( long invocationId ) {
		
		if( invocationId < 0 )
			throw new RuntimeException( "Invocation id must not be less than 0." );
		
		this.invocationId = invocationId;
	}
	
	public void setKey( String key ) {
		
		if( key == null )
			throw new NullPointerException( "Key must not be null." );
		
		if( key.isEmpty() )
			throw new RuntimeException( "Key must not be empty." );
		
		this.key = key.replace( "\n", "\\n");
	}
	
	public void setPayload( String payload ) {
		
		if( payload == null )
			throw new NullPointerException( "Payload must not be null." );
		
		if( payload.isEmpty() )
			throw new RuntimeException( "Payload must not be empty." );
		
		this.payload = payload.replace( "\n", "\\n" );
	}
	
	public void setTaskName( String taskName ) {
		
		if( taskName == null )
			throw new NullPointerException( "Task name must not be null." );

		if( taskName.isEmpty() )
			throw new RuntimeException( "Task name must not be empty." );
		
		this.taskName = taskName;
	}
	
	public void setTaskNodeId( int taskNodeId ) {
		
		if( taskNodeId < 0 )
			throw new RuntimeException( "Task node id must not be less than 0." );
		
		this.taskNodeId = taskNodeId;
	}
	
	public void setTimeStamp( long timeStamp ) {
		this.timeStamp = timeStamp;
	}
	
	public void setWfName( String wfName ) {
		
		if( wfName == null )
			throw new NullPointerException( "Workflow name must not be null." );
		
		if( wfName.isEmpty() )
			throw new RuntimeException( "Workflow name must not be empty." );
		
		this.wfName = wfName;
	}
	
	@Override
	public String toString() {
		
		StringBuffer buf;
		
		buf = new StringBuffer();
		
		buf.append( timeStamp );
		buf.append( DELIM_COLUMN ).append( dagId );
		buf.append( DELIM_COLUMN ).append( wfName );
		buf.append( DELIM_COLUMN ).append( taskNodeId );
		buf.append( DELIM_COLUMN ).append( invocationId );
		buf.append( DELIM_COLUMN ).append( taskName );		
		buf.append( DELIM_COLUMN ).append( key );
		buf.append( DELIM_COLUMN ).append( payload );
		buf.append( "\n" );
		
		return buf.toString();
	}
}
