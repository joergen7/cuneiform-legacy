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

/** A data node represents a literal string in Cuneiform.
 * 
 * From the way data nodes are defined in the Cuneiform source code, can be
 * derived, that it must always have exactly one child.
 * 
 * @author Jorgen Brandt
 *
 */
public class DataNode extends WfElement {
	
	private String literal;
	private WfElement child;
	private boolean stage;

	/** Constructs an instance of DataNode.
	 * 
	 * @param wfName The name of the workflow this data node appears in.
	 * @param id a unique number.
	 * @param dataName The literal string, this data node represents.
	 */
	public DataNode( String wfName, String dataName, boolean stage ) {
		
		super( wfName );
		setLiteral( dataName );
		setStage( stage );
	}

	@Override
	public void addChild( WfElement childNode ) {
		
		if( childNode == null )
			throw new NullPointerException(
				"Child connectable must not be null." );
		
		if( childNode instanceof DataNode )
			throw new RuntimeException(
				"A data node cannot have another data node as its child." );
		
		if( childNode instanceof DefTaskNode )
			throw new RuntimeException(
				"A data node cannot have a deftask node as its child." );
		
		this.child = childNode;
	}
	
	@Override
	public boolean childSetContains( WfElement element ) {
		return child.equals( element );
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
		return "datanode"+getId();
	}

	@Override
	public String getDotNode() {
		return getDotId()+" [label=\""+getLiteral()+"\"];";
	}
	
	/** Retrieves the literal string, this data node represents.
	 * 
	 * @return A literal string.
	 */
	public String getLiteral() {		
		return literal;
	}
	
	@Override
	public List<WfElement> getParentList() {
		return new LinkedList<>();
	}
	
	@Override
	public boolean isStage( int outputChannel ) {
		
		if( outputChannel != 0 )
			throw new RuntimeException(
				"Data node does not provide output channel "+outputChannel
				+"." );
		
		return stage;
	}

	@Override
	public int nOutputChannel() {
		return 1;
	}
	
	@Override
	public boolean parentListContains( WfElement element ) {
		return false;
	}

	@Override
	public int parentListSize() {
		return 0;
	}

	/** Set the literal string that this data node represents.
	 * 
	 * @param literal The literal string to be associated to this data node.
	 */
	public void setLiteral( String literal ) {
		
		if( literal.isEmpty() )
			throw new NullPointerException( "Data name must not be null." );
		
		if( literal.isEmpty() )
			throw new RuntimeException( "Data name must not be empty." );
		
		this.literal = literal;
	}
	
	public void setStage( boolean stage ) {
		this.stage = stage;
	}

	@Override
	public String toString() {
		return literal;
	}

	@Override
	public DataList getDataList( int outputChannel ) {
		
		DataList resultList;
		
		if( outputChannel != 0 )
			throw new RuntimeException(
				"Anonymous junction has only outputChannel 0, tried to access "
				+"channel "+outputChannel+"." );
		
		resultList = new DataList();
		
		resultList.add( new DataItem( literal ) );
		
		return resultList;
	}



}
