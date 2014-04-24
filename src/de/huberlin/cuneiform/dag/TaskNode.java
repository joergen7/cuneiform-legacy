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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.huberlin.cuneiform.common.Constant;
import de.huberlin.cuneiform.language.DefTask;

public class TaskNode extends WfElement {
	
	private Map<String,WfElement> parentMap;
	private List<WfElement> childList;
	private CuneiformDag dag;
	private List<Invocation> invocationList;

	public TaskNode( CuneiformDag dag, String wfName ) {
		
		super( wfName );
		parentMap = new HashMap<>();
		childList = new LinkedList<>();
		setDag( dag );
		invocationList = new LinkedList<>();
		
	}
	
	@Override
	public void addChild( WfElement childNode ) {
		addChild( childNode, 0 );
	}
	
	public void addChild( WfElement child, int index ) {

		if( child == null )
			throw new NullPointerException( "Child must not be null." );
		
		if( child instanceof DataNode )
			throw new RuntimeException(
				"A data node cannot have another data node as its child." );
		
		if( child instanceof DefTaskNode )
			throw new RuntimeException(
				"A data node cannot have a deftask node as its child." );
		
		if( index < 0 )
			throw new RuntimeException(
				"Channel index must not be smaller than 0." );

		childList.add( child );
	}
	
	public void addParent( WfElement parent, String paramName ) {
		
		if( parent == null )
			throw new NullPointerException( "Parent must not be null." );
		
		if( paramName == null )
			throw new NullPointerException(
				"Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		parentMap.put( paramName, parent );
	}
	
	@Override
	public boolean childSetContains( WfElement element ) {		
		return childList.contains( element );
	}
	
	@Override
	public int childSetSize() {
		return childList.size();
	}
	
	public boolean containsParam( String paramName ) {
		
		if( paramName == null )
			throw new NullPointerException(
				"Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		return parentMap.containsKey( paramName );
	}
	
	public List<WfElement> getChildList() {
		return Collections.unmodifiableList( childList );
	}
	
	@Override
	public Set<WfElement> getChildSet() {
		
		Set<WfElement> set;
		
		set = new HashSet<>();
		set.addAll( childList );
		
		return set;
	}
	
	public CuneiformDag getCuneiformDag() {
		return dag;
	}
	
	public String getDagId() {
		return dag.getDagId();
	}
	
	public CuneiformDag getDag() {
		return dag;
	}
	
	@Override
	public String getDotId() {
		return "tasknode"+getId();
	}
	
	@Override
	public String getDotNode() {
		return getDotId()+" [label=\"\",shape=box];";
	}
	
	public String getLang() throws NotDerivableException {
		return getDefTaskExample().getLangLabel();
	}

	public List<WfElement> getNonTaskParentList() {
		
		List<WfElement> set;
		
		set = new LinkedList<>();
		
		for( String paramName : parentMap.keySet() )
			if( !paramName.equals( Constant.TOKEN_TASK ) )
				set.add( parentMap.get( paramName ) );
		
		return set;
	}
	
	public int getOutputChannelForWfElement( WfElement child ) {
		
		int i;
		i = childList.indexOf( child );
		
		if( i < 0 )
			throw new RuntimeException( "Child element is not bound to any output channel." );
		
		return i;
	}
	
	public WfElement getParam( String paramName ) {
		
		WfElement ret;
		
		if( paramName == null )
			throw new NullPointerException(
				"Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		ret = parentMap.get( paramName );
		
		if( ret == null )
			throw new NullPointerException( "Parameter with name '"+paramName+"' not found." );
		
		return ret;
	}
	
	public Map<String,WfElement> getParamMap() {
		return Collections.unmodifiableMap( parentMap );
	}
	
	public List<WfElement> getParamList() {
		
		List<WfElement> paramList;
		
		paramList = new LinkedList<>();
		for( String paramName : parentMap.keySet() )
			paramList.add( parentMap.get( paramName ) );
		
		return paramList;
	}
	
	public Set<String> getParamNameSet() {
		return parentMap.keySet();
	}

	@Override
	public List<WfElement> getParentList() {
		
		List<WfElement> set;
		
		set = new LinkedList<>();
		set.addAll( parentMap.values() );
		
		return set;
	}
	
	public WfElement getTaskParent() {
		
		for( String paramName : parentMap.keySet() )
			if( paramName.equals( Constant.TOKEN_TASK ) )
				return parentMap.get( paramName );
		
		throw new RuntimeException( "No parameter '"+Constant.TOKEN_TASK+"' found." );
	}
	
	public int outputIndexOf( String outputName ) throws NotDerivableException {
		return getDefTaskExample().outputIndexOf( outputName );
	}
	
	public boolean hasDefTaskExample() {
		
		try {
			getDefTaskExample();
			return true;
		}
		catch( NotDerivableException e ) {
			return false;
		}
	}
	
	@Override
	public boolean isStage( int outputChannel )throws NotDerivableException {		
		return getDefTaskExample().isOutputStage( outputChannel );
	}
	
	@Override
	public int nOutputChannel() throws NotDerivableException {
		return this.getDefTaskExample().nOutputChannel();
	}
	
	public int outputTypeOf( int outputIndex ) throws NotDerivableException {
		return getDefTaskExample().outputTypeOf( outputIndex );
	}
	
	public int outputTypeOf( String outputName ) throws NotDerivableException {
		return outputTypeOf( outputIndexOf( outputName ) );
	}
	
	@Override
	public boolean parentListContains( WfElement element ) {
		return parentMap.values().contains( element );
	}
	
	@Override
	public int parentListSize() {
		return parentMap.size();
	}
	
	public void setDag( CuneiformDag dag ) {
		
		if( dag == null )
			throw new NullPointerException( "Cuneiform DAG must not be null." );
		
		this.dag = dag;
	}
	
	public DataList getTaskItemList() {
		return getTaskParent().getDataList( 0 );
	}
	
	public DefTask getDefTaskExample() throws NotDerivableException {
		
		DataList taskItemList;
		DefTaskNode defTaskNode;

		taskItemList = getTaskItemList();
		
		if( taskItemList.isEmpty() )
			throw new RuntimeException( "No task item given." );
		
		// get task definition representative for the task node's prototype
		defTaskNode = dag.getDefTaskNode(
			taskItemList.getRealization().getValue() );
	
		return defTaskNode.getDefTask();
	}
	
	@Override
	public DataList getDataList( int outputChannel ) {
		
		DataList dataList;
		TaskReference ref;
		
		dataList = new DataList();
		
		ref = new TaskReference( this, outputChannel );
		dataList.add( ref );
		
		return dataList;
	}
	
	public void addInvocation( Invocation invocation ) {
		
		if( invocation == null )
			throw new NullPointerException( "Invocation must not be null." );
		
		invocationList.add( invocation );
	}
	
	
	public int getOutputType( int outputChannel ) throws NotDerivableException {
		return getDefTaskExample().getOutputType( outputChannel );
	}
	
	public WfElement getChild( int i ) {
		return childList.get( i );
	}
	
	public boolean hasInvocationList() {
		return !invocationList.isEmpty();
	}
	
	public List<Invocation> getInvocationList() {
		return Collections.unmodifiableList( invocationList );
	}
	
}
