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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/** An anonymous junction represents the union of two or more data items.
 * 
 * Anonymous junctions occur when the result of a union is not stored in a
 * named variable but directly used in an apply statement. In the graphical
 * representation of a workflow DAG anonymous junctions appear as points whose
 * out-going edge is connected to a task node. 
 * 
 * @author jorgen
 *
 */
public class AnonymousJunction
extends WfElement implements ParentAddable {
	
	private List<WfElement> parentList;
	private TaskNode child;

	/** Constructs a new junction instance.
	 * 
	 * @param wfName The name of the workflow that contains this junction.
	 * @param id A unique number.
	 */
	public AnonymousJunction( String wfName ) {
		
		super( wfName );
		
		parentList = new LinkedList<>();
	}
	
	@Override
	public void addChild( WfElement childNode ) {
		
		if( childNode == null )
			throw new NullPointerException(
				"Child connectable must not be null." );
		
		if( !( childNode instanceof TaskNode ) )
			throw new RuntimeException(
				"Anonymous junction's child must be a task node." );
		
		this.child = ( TaskNode )childNode;
	}
	
	@Override
	public void addParent( WfElement parent ) {
		
		if( parent == null )
			throw new NullPointerException( "Parent must not be null." );
		
		if( parent instanceof AnonymousJunction )
			throw new RuntimeException(
				"Anonymous junction cannot have another anonymous junction as "
				+"its parent." );
		
		parentList.add( parent );
	}

	@Override
	public boolean childSetContains( WfElement element ) {
		return element.equals( child );
	}

	@Override
	public int childSetSize() {
		
		if( child == null )
			return 0;
		
		return 1;
	}
	
	@Override
	public Set<WfElement> getChildSet() {
		
		Set<WfElement> set;
		
		set = new HashSet<>();
		if( child != null )
			set.add( child );
		
		return set;
	}
	
	@Override
	public String getDotId() {
		return "junction"+getId();
	}
	
	@Override
	public String getDotNode() {
		return getDotId()+" [label=\"\",shape=point];";
	}

	@Override
	public List<WfElement> getParentList() {
		return Collections.unmodifiableList( parentList );
	}

	@Override
	public boolean isStage( int outputChannel )
	throws NotDerivableException {
				
		if( parentList.isEmpty() )
			throw new RuntimeException(
				"Parent list of anonymous junction must not be empty." );
		
		if( outputChannel != 0 )
			throw new RuntimeException(
				"Anonymous junction does not provide output channel "
				+outputChannel+"." );
		
		return parentList.get( 0 ).isStage( 0 );
	}

	@Override
	public int nOutputChannel() {
		return 1;
	}

	@Override
	public int parentListSize() {
		return parentList.size();
	}

	@Override
	public boolean parentListContains( WfElement element ) {
		return parentList.contains( element );
	}

	@Override
	public DataList getDataList( int outputChannel ) {
		
		DataList resultList;
		
		if( outputChannel != 0 )
			throw new IndexOutOfBoundsException(
				"Anonymous junction has only outputChannel 0, tried to access "
				+"channel "+outputChannel+"." );
		
		resultList = new DataList();
		
		for( WfElement element : parentList )
			resultList.add( element.getDataList( 0 ) );
		
		return resultList;
	}
	

}
