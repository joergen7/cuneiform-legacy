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
import java.util.List;
import java.util.Set;

/**
 * Workflow elements are anything in a workflow DAG that can be the source or
 * sink of a data dependency edge. Workflow elements are always doubly linked.
 * I.e., when exploring the graph it is up to you, whether you start from a
 * child and traverse all its parents or whether you start from a parent and
 * traverse all its children.
 * 
 * @see WfElement
 * 
 * @author Jorgen Brandt
 */
public abstract class WfElement implements DotElement {

	private static int runningId;
	
	private String wfName;
	private int id;
	
	public WfElement( String wfName ) {
		setWfName( wfName );
		setId();
	}
	
	public abstract void addChild( WfElement childNode );
	
	public abstract boolean childSetContains( WfElement element );
	
	public abstract int childSetSize();
	
	@Override
	public boolean equals( Object o ) {
		
		WfElement other;
		
		if( !( o instanceof WfElement ) )
			return false;
		
		other = ( WfElement )o;
		
		return id == other.getId();
	}
	
	/** Returns the set of child Connectables associated to this parent
	 * 
	 * @return The set of associated child Connectables
	 */
	public abstract Set<WfElement> getChildSet();
	
	public abstract DataList getDataList( int outputChannel );
	
	public Set<WfElement> getDeepParentSet() {
		
		Set<WfElement> result;
		List<WfElement> shallowParentSet;
		
		result = new HashSet<>();
		shallowParentSet = getParentList();
		
		result.addAll( shallowParentSet );
		for( WfElement parent : shallowParentSet )
			result.addAll( parent.getDeepParentSet() );
		
		return result;
	}
	
	public Set<TaskNode> getDeepTaskNodeParentSet() {
		
		Set<TaskNode> depList = new HashSet<>();
		
		if ( this instanceof TaskNode )
			
			depList.add( ( TaskNode )this );
		
		for( WfElement parent : getParentList() )
			depList.addAll( parent.getDeepTaskNodeParentSet() );
		
		return depList;
	}
	

	public int getId() {
		return id;
	}
	
	/** Returns the set of parent WfElements associated to this child
	 * 
	 * @return The set of associated parent Connectables
	 */
	public abstract List<WfElement> getParentList();
	
	public String getWfName() {
		return wfName;
	}
	
	@Override
	public int hashCode() {
		return id%23;
	}

	public abstract boolean isStage( int outputChannel )
	throws NotDerivableException;
	
	public abstract int nOutputChannel() throws NotDerivableException;
	
	public abstract boolean parentListContains( WfElement element );
	
	public abstract int parentListSize();

	public void setWfName( String wfName ) {
		
		if( wfName == null )
			throw new NullPointerException( "Workflow name must not be null." );
		
		if( wfName.isEmpty() )
			throw new RuntimeException( "Workflow name must not be empty." );
		
		this.wfName = wfName;
	}
	
	private synchronized void setId() {
		id = runningId++;
	}
	
}
