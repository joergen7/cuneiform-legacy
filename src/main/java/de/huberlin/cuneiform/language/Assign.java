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

public class Assign {

	private List<String> varList;
	private List<Expression> exprList;
	
	public Assign() {
		varList = new LinkedList<>();
		exprList = new LinkedList<>();
	}
	
	public void addExpression( Expression expr ) {
		
		if( expr == null )
			throw new NullPointerException( "Expression must not be null." );
		
		exprList.add( expr );
	}
	
	public void addVar( String varname ) {
		
		if( varname == null )
			throw new NullPointerException( "Variable name must not be null." );
		
		if( varname.isEmpty() )
			throw new RuntimeException( "Variable name must not be empty." );
		
		varList.add( varname );
	}
	
	public void addExpression( List<Expression> set ) {
		
		if( set == null )
			throw new NullPointerException( "Expression must not be null." );
		
		if( set.isEmpty() )
			throw new RuntimeException( "Expression set must not be emtpy." );
		
		exprList.addAll( set );
	}
	
	public boolean containsVar( String key ) {
		return varList.contains( key );
	}
	
	public int exprSetSize() {
		return exprList.size();
	}
	
	public List<Expression> getExprList() {
		return exprList;
	}
	
	public List<String> getVarList() {
		return varList;
	}
	
	@Override
	public String toString() {
		
		String ret;
		
		ret = "";
		for( String var : varList )
			ret += var+" ";
		
		ret += "=";
		
		for( Expression expr : exprList )
			ret += " "+expr;
		
		return ret;
	}
	
	public int varListSize() {
		return varList.size();
	}
}
