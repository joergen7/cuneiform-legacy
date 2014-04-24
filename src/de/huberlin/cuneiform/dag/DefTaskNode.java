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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import de.huberlin.cuneiform.language.CorrelParam;
import de.huberlin.cuneiform.language.DefTaskOutput;
import de.huberlin.cuneiform.language.DefTask;
import de.huberlin.cuneiform.language.DefTaskParam;

public class DefTaskNode extends WfElement {

	private Set<WfElement> childSet;
	private List<Boolean> stageList;
	private DefTask defTask;
	private String body;
	
	public DefTaskNode( String wfName, DefTask defTask, String body ) {
		
		super( wfName );
		setDefTask( defTask );
		childSet = new HashSet<>();
		stageList = new LinkedList<>();
		setBody( body );
	}
	

	@Override
	public void addChild( WfElement child ) {
		
		if( child == null )
			throw new NullPointerException( "Child node must not be null." );
		
		if( child instanceof DataNode )
			throw new RuntimeException(
				"A deftask node cannot have a data node as its child." );
		
		if( child instanceof DefTaskNode )
			throw new RuntimeException(
				"A deftask node cannot have another deftask node as its "
				+"child." );
		
		childSet.add( child );
	}
	
	@Override
	public boolean childSetContains( WfElement element ) {
		return childSet.contains( element );
	}

	@Override
	public int childSetSize() {
		return childSet.size();
	}

	public boolean containsOutput( String outputName ) {
		return defTask.containsOutputWithName( outputName );
	}
	
	public boolean containsParam( String paramName ) {
		return defTask.containsExplicitParamWithName( paramName );
	}
	
	public String getBody() {
		return body;
	}
	
	@Override
	public Set<WfElement> getChildSet() {
		
		Set<WfElement> set;
		
		set = new HashSet<>();
		set.addAll( childSet );
		
		return set;
	}
	
	public DefTask getDefTask() {
		return defTask;
	}
	
	@Override
	public String getDotId() {
		return "namedjunction"+getId();
	}
	
	@Override
	public String getDotNode() {
		return getDotId()+" [label=\""+getTaskName()+"\",shape=plaintext];";
	}
	
	public Set<DefTaskParam> getNonTaskParamSet() {
		return defTask.getNonTaskParamSet();
	}
	
	public List<DefTaskOutput> getOutputList() {
		return defTask.getOutputList();
	}
	
	/** Returns the names of all outputs in the order in which they are bound.
	 * 
	 * @return The list of output variable names.
	 */
	public List<String> getOutputNameList() {
		return defTask.getOutputNameList();
	}
	
	/** Returns the names of all inputs.
	 * 
	 * @return The set of input parameter names.
	 */
	public Set<String> getParamNameSet() {
		return defTask.getParamNameSet();
	}
	
	public Set<DefTaskParam> getParamSet() {
		return defTask.getParamSet();
	}
	
	@Override
	public List<WfElement> getParentList() {
		return new LinkedList<>();
	}

	public String getTaskName() {
		return defTask.getTaskName();
	}
	
	public CorrelParam getTaskParam() {
		return defTask.getTaskParam();
	}
	
	public boolean isOutputReduce( String outputName ) {
		return defTask.isOutputReduce( outputName );
	}
	
	public boolean isOutputReduce( int outputChannel ) {
		return defTask.isOutputReduce( outputChannel );
	}
	
	public boolean isOutputStage( String outputName ) {
		return defTask.isOutputStage( outputName );
	}
	
	public boolean isOutputStage( int outputIndex ) {
		return stageList.get( outputIndex );
	}
	
	public boolean isParamReduce( String param ) {
		return defTask.isParamReduce( param );
	}
	
	@Override
	public boolean isStage( int outputChannel ) {
		
		if( outputChannel != 0 )
			throw new RuntimeException( "DefTaskNode does not provide output channel "+outputChannel+"." );
		
		return false;
	}

	@Override
	public int nOutputChannel() {
		return 1;
	}


	public int outputIndexOf( String outputName ) {
		return defTask.outputIndexOf( outputName );
	}
	
	public int outputListSize() {
		return defTask.nOutputChannel();
	}
	
	public int outputTypeOf( int outputIndex ) {
		return defTask.outputTypeOf( outputIndex );
	}

	@Override
	public boolean parentListContains( WfElement element ) {
		return false;
	}

	@Override
	public int parentListSize() {
		return 0;
	}
	
	public void setBody( String body ) {
		
		if( body == null )
			throw new NullPointerException( "Body must not be null." );
		
		this.body = body;
	}

	public void setDefTask( DefTask defTask ) {
		
		if( defTask == null )
			throw new NullPointerException( "Task definition must not be null." );
		
		this.defTask = defTask;
	}


	@Override
	public DataList getDataList( int outputChannel ) {

		DataList resultList;
		
		if( outputChannel != 0 )
			throw new RuntimeException(
				"Task definition node has only outputChannel 0, tried to access"
				+" channel "+outputChannel+"." );
		
		resultList = new DataList();
		resultList.add( new DataItem( getTaskName() ) );
		
		return resultList;
	}




}
