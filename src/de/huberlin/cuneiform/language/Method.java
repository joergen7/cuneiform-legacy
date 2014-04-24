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

package de.huberlin.cuneiform.language;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.antlr.v4.runtime.Token;

public abstract class Method extends RawBodyContainer {

	private Set<String> varSet;
	private int line;
	
	public Method( int line ) {
		varSet = new HashSet<>();
		setLine( line );
	}
	
	public void addVar( Token idToken ) {
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		addVar( idToken.getText() );
	}
	
	public void addVar( String varName ) {
		
		if( varName == null )
			throw new NullPointerException( "Variable name must not be null." );
		
		if( varName.isEmpty() )
			throw new RuntimeException( "Variable name must not be empty." );
		
		varSet.add( varName );		
	}
	
	public void clearVarSet() {
		varSet.clear();
	}
	
	public boolean containsVar( String var ) {
		return varSet.contains( var );
	}
	
	public int getLine() {
		return line;
	}
	
	public Set<String> getVarSet() {
		return Collections.unmodifiableSet( varSet );
	}
	
	public void setLine( int line ) {
		
		if( line < 1 )
			throw new RuntimeException( "Line must be a positive number." );
		
		this.line = line;
	}

	public int varSetSize() {
		return varSet.size();
	}
	
}
