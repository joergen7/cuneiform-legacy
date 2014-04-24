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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import de.huberlin.cuneiform.common.Constant;
import de.huberlin.cuneiform.language.CorrelParam;
import de.huberlin.cuneiform.language.DefTaskParam;
import de.huberlin.cuneiform.language.ReduceParam;

public class CombiTable {

	public static final int VALUE_REDUCE = 0;

	private List<DefTaskParam> paramList;
	private List<Integer> cntList;
	
	public CombiTable() {	
		paramList = new LinkedList<>();
		cntList = new LinkedList<>();
	}
	
	public void addDefTaskParam( DefTaskParam param ) {
		
		if( param == null )
			throw new NullPointerException(
				"Task definition parameter must not be null." );
		
		paramList.add( param );
		
		if( param instanceof ReduceParam ) {
			
			cntList.add( 1 );
			return;
		}
		
		if( param instanceof CorrelParam ) {
			
			cntList.add( null );
			return;
		}
		
		throw new RuntimeException( "Parameter type not recognized." );
	}
	
	public void addDefTaskParam( Set<DefTaskParam> paramSet ) {
		
		if( paramSet == null )
			throw new NullPointerException(
				"Task definition parameter set must not be null." );
		
		for( DefTaskParam param : paramSet )
			addDefTaskParam( param );
	}
	
	public boolean isAllSizeKnown() {
		
		int i;
		
		for( i = 0; i < paramList.size(); i++ )
			if( cntList.get( i ) == null )
				return false;

		return true;
	}
	
	public void setSize( String paramName, int cnt ) {
		
		int i;
		Integer j;
		DefTaskParam param;
		
		for( i = 0; i < paramList.size(); i++ ) {
		
			param = paramList.get( i );
			
			if( param.containsItemWithValue( paramName ) ) {
				
				if( param instanceof ReduceParam )
					return;
			
				j = cntList.get( i );
				if( j != null )
					if( j != cnt )
						throw new RuntimeException(
							"Inconsistent size information. Parameter '"
							+paramName+"' is set size "+cnt
							+" but another source sais it's "+j+"." );
				
				cntList.set( i, cnt );
				return;
			}
		
		}
		
		throw new RuntimeException(
			"A parameter with the name '"+paramName
			+"' has never been registered." );
	}
	
	public int size() {
		
		int s;
		int i, j;
		Integer f;
		
		s = 1;
		
		if( paramList.size() != cntList.size() )
			throw new RuntimeException( "Inconsistent data structure. paramList and cntList must be of the same size." );
		
		for( i = 0; i < paramList.size(); i++ ) {

			f = cntList.get( i );
			
			if( f == null ) {
				
				for( j = 0; j < paramList.size(); j++ ) {
					System.err.print( "|"+paramList.get( j )+"|="+cntList.get( j ) );
					if( j == i )
						System.err.println( "*" );
					else
						System.err.println();
				}
				
				throw new RuntimeException( "Size information for parameter '"+paramList.get( i )+"' incomplete." );
			}
			
			s *= f;
		}
		
		return s;
	}
	
	public int indexOf( String paramName, int iteration ) {

		int i, j;
		DefTaskParam param;
		
		i = iteration;
		
		for( j = 0; j < paramList.size(); j++ ) {
			
			param = paramList.get( j );
			
			if( param instanceof ReduceParam ) {
				
				if( param.containsItemWithValue( paramName ) )
					throw new RuntimeException(
						"Cannot enumerate reduce parameter '"+paramName+"'." );
				
				continue;
			}
			
			if( param.containsItemWithValue( paramName ) )	
				return i%cntList.get( j );
			
			i /= cntList.get( j );
		}
		
		throw new RuntimeException(
			"Parameter with name '"+paramName+"' not found." );
	}
	
	public int indexOfTaskParam( int iteration ) {
		return indexOf( Constant.TOKEN_TASK, iteration );
	}
}
