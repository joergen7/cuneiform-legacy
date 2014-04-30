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
import java.util.Set;


public class ResolvedInvocationReference implements Resolveable {

	private int idx;
	private Invocation invoc;
	private int outputChannel;

	public ResolvedInvocationReference( Invocation invoc, int outputChannel, int idx ) {
		setOutputChannel( outputChannel );
		setIndex( idx );
		setInvocation( invoc );
	}

	@Override
	public String getValue() throws NotDerivableException {
		return invoc.getDataList( outputChannel ).get( idx ).getValue();
	}
	
	public int getIndex() {
		return idx;
	}
	
	@Override
	public Invocation getInvocation() {
		return invoc;
	}
	
	public int getOutputChannel() {
		return outputChannel;
	}
	
	public void setIndex( int idx ) {
		
		if( idx < 0 )
			throw new RuntimeException( "Index must not be less than 0." );
		
		this.idx = idx;
	}

	@Override
	public boolean isInvocation() {
		return true;
	}
	
	public void setInvocation( Invocation invoc ) {
		
		if( invoc == null )
			throw new NullPointerException( "Invocation must not be null." );
		
		this.invoc = invoc;
	}
	
	@Override
	public String toString() {
		try {
			return "'"+getValue()+"'";
		}
		catch( NotDerivableException e ) {
			return "?";
		}
	}

	public void setOutputChannel( int outputChannel ) {
		
		if( outputChannel < 0 )
			throw new RuntimeException( "Output channel must be greater or equal to 0." );
				
		this.outputChannel = outputChannel;
	}

	@Override
	public Set<Invocation> getInvocationSet() {
		
		Set<Invocation> set;
		
		set = new HashSet<>();
		set.add( invoc );
		
		return set;
	}

}
