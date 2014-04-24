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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.Token;

public class IdExpression implements Expression {
	
	private String value;
	
	public IdExpression( Token idToken ) {
		this( idToken.getText() );
	}
	
	public IdExpression( String value ) {
		setValue( value );
	}
	
	public void setValue( String value ) {
		
		if( value == null )
			throw new NullPointerException( "Value string must not be null." );
		
		if( value.isEmpty() )
			throw new RuntimeException( "Value string must not be empty." );
		
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}

	@Override
	public List<Expression> substitute(
		Map<String,DefMacro> defMacroMap, Map<String,List<Expression>> substitMap ) {

		List<Expression> result;

		if( substitMap.containsKey( value ) ) {
			
			result = substitMap.get( value );
			if( result == null )
				throw new NullPointerException( "Substitution expression must not be null." );
			
			return result;
		}
		
		result = new LinkedList<>();
		result.add( this );
		
		return result;
	}
	
	@Override
	public String toString() {
		return value;
	}

}
