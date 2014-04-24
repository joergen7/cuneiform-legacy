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

public class NamedJunction
extends WfElement implements ParentAddable {
	
	private String junctionName;
	private List<WfElement> parentList;
	private Set<WfElement> childSet;
	private int parentOutputChannel;
	
	public NamedJunction( String wfName, String junctionName, int parentOutputChannel ) {
		
		super( wfName );
		
		setJunctionName( junctionName );
		setParentOutputChannel( parentOutputChannel );
		
		parentList = new LinkedList<>();
		childSet = new HashSet<>();
	}
	
	@Override
	public void addChild( WfElement child ) {
		
		if( child == null )
			throw new NullPointerException( "Child must not be null." );
		
		if( child instanceof DataNode )
			throw new RuntimeException(
				"Cannot set data node as a named junction's child." );
		
		if( child instanceof DefTaskNode )
			throw new RuntimeException(
				"Cannot set deftask node as named junction's child." );
		
		childSet.add( child );
	}
	
	@Override
	public void addParent( WfElement parent ) {
		
		if( parent == null )
			throw new NullPointerException( "Parent must not be null." );
		
		if( parent instanceof AnonymousJunction )
			throw new RuntimeException(
				"Cannot set anonymous junction as a named junction's parent." );
		
		parentList.add( parent );
	}
	
	@Override
	public boolean childSetContains( WfElement element ) {
		return childSet.contains( element );
	}

	@Override
	public int childSetSize() {
		return childSet.size();
	}

	@Override
	public String getDotId() {
		return "namedjunction"+getId();
	}
	
	@Override
	public String getDotNode() {
		return getDotId()+" [label=\""+getJunctionName()+"\",shape=plaintext];";
	}
	
	public String getJunctionName() {
		
		String s;
		
		s = junctionName;
		if( s == null )
			throw new NullPointerException( "Junction name must not be null." );
		
		return s;
	}
	
	@Override
	public Set<WfElement> getChildSet() {
		return Collections.unmodifiableSet( childSet );
	}

	@Override
	public List<WfElement> getParentList() {
		return Collections.unmodifiableList( parentList );
	}
	
	public int getParentOutputChannel() {
		return parentOutputChannel;
	}

	@Override
	public boolean isStage( int outputChannel )
	throws NotDerivableException {
		
		if( outputChannel != 0 )
			throw new RuntimeException(
				"Named junction does not provide output channel "
				+outputChannel+"." );
		
		if( parentList.isEmpty() )
			throw new RuntimeException( "Parent list must not be empty." );
		
		return parentList.get( 0 ).isStage( 0 );
	}

	@Override
	public int nOutputChannel() {
		return 1;
	}

	@Override
	public boolean parentListContains( WfElement element ) {
		return parentList.contains( element );
	}

	@Override
	public int parentListSize() {
		return parentList.size();
	}

	public void setJunctionName( String junctionName ) {
		
		if( junctionName == null )
			throw new NullPointerException( "Junction name must not be null." );
		
		if( junctionName.isEmpty() )
			throw new RuntimeException( "Junction name must not be empty." );
		
		this.junctionName = junctionName;
	}
	
	public void setParentOutputChannel( int parentOutputChannel ) {
		this.parentOutputChannel = parentOutputChannel;
	}
	
	@Override
	public String toString() {
		return junctionName;
	}

	@Override
	public DataList getDataList( int outputChannel ) {
		
		DataList ancestorList;
		
		if( outputChannel != 0 )
			throw new RuntimeException(
				"Anonymous junction has only outputChannel 0, tried to access "
				+"channel "+outputChannel+"." );
				
		ancestorList = new DataList();
		
		for( WfElement element : parentList )
			ancestorList.add( element.getDataList( parentOutputChannel ) );
		
		return ancestorList;
	}
	

}
