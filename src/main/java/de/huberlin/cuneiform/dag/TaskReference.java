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

public class TaskReference extends Reference implements DataListContent {
	
	private TaskNode taskNode;

	public TaskReference( TaskNode taskNode, int outputChannel ) {
		
		super( outputChannel );
		
		setTaskNode( taskNode );
	}
	
	public void setTaskNode( TaskNode taskNode ) {
		
		if( taskNode == null )
			throw new NullPointerException( "Task node must not be null." );
		
		this.taskNode = taskNode;
	}

	@Override
	public int size() throws NotDerivableException {
		
		int s;
		
		if( !taskNode.hasInvocationList() )
			throw new NotDerivableException(
				"Task node size unknown unless invocations are enumerated." );
		
		
		s = 0;
		for( Invocation invoc : taskNode.getInvocationList() )
			s += invoc.size( getOutputChannel() );
		
		return s;
	}

	@Override
	public Resolveable get( int idx ) throws NotDerivableException {
		
		int i;
		int outputChannel;
		
		if( idx < 0 )
			throw new IndexOutOfBoundsException(
				"Index must not be smaller than 0." );
		
		if( !taskNode.hasInvocationList() )
			throw new NotDerivableException(
				"Can't return data item with no invocation registered." );
		
		i = idx;
		outputChannel = getOutputChannel();
		
		for( Invocation invocation : taskNode.getInvocationList() ) {
			
			if( i-invocation.size( outputChannel ) < 0 )
				return invocation.getResolveable( outputChannel, i );
			
			i -= invocation.size( outputChannel );
		}
		
		throw new IndexOutOfBoundsException(
			"Index "+idx+" exceeds size of task node reference." );
	}
	
	public TaskNode getTaskNode() {
		return taskNode;
	}
	
	public Invocation getInvocation( int idx ) {
		return taskNode.getInvocationList().get( idx );
	}
	
	public int getTaskNodeId() {
		return taskNode.getId();
	}
	
	public List<InvocationReference> getInvocationReferenceList() throws NotDerivableException {

		List<InvocationReference> list;
		int outputChannel;
		
		if( !taskNode.hasInvocationList() )
			throw new NotDerivableException( "No invocations were ever registered." );
		
		list = new LinkedList<>();
		outputChannel = getOutputChannel();
		
		for( Invocation invoc : taskNode.getInvocationList() )
			list.add( new InvocationReference( outputChannel, invoc ) );
		
		return list;
	}
	
	public List<Invocation> getInvocationList() {
		
		return taskNode.getInvocationList();
		

	}
	
	@Override
	public String toString() {
		
		int i, n;
		String ret;
		
		try {
			n = size();
		}
		catch( NotDerivableException e ) {
			return "/List of unknown size/";
		}
		
		ret = "";
		
		for( i = 0; i < n; i++ ) {
			
			if( !ret.isEmpty() )
				ret += ", ";
			
			try {
				ret += get( i ).getValue();
			}
			catch( NotDerivableException e ) {
				ret += "?";
			}
		}
		
		return ret;
	}

	@Override
	public Set<Invocation> getInvocationSet() {
		
		Set<Invocation> set;
		
		set = new HashSet<>();
		set.addAll( taskNode.getInvocationList() );
		
		return set;
	}
}
