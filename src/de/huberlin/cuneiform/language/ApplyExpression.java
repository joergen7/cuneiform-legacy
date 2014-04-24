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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.Token;

public class ApplyExpression implements Expression {

	private Map<String,List<Expression>> paramMap;
	
	public ApplyExpression() {		
		paramMap = new HashMap<>();
	}
	
	public void addParam( String param, List<Expression> exprList ) {
				
		if( param == null )
			throw new NullPointerException( "Parameter name must not be null." );
		
		if( param.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		if( exprList == null )
			throw new NullPointerException( "Expression list must not be null." );
		
		if( exprList.isEmpty() )
			throw new RuntimeException( "Expression list must not be empty." );
		
		paramMap.put( param, exprList );
	}
	
	public void addParam( Token idToken, List<Expression> exprSet ) {

		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		addParam( idToken.getText(), exprSet );
	}
	
	public boolean containsParam( String param ) {
		return paramMap.containsKey( param );
	}
	
	public List<Expression> getExprForParam( String paramName ) {
		
		List<Expression> exprList;
		
		if( paramName == null )
			throw new NullPointerException( "Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		exprList = paramMap.get( paramName );
		if( exprList == null )
			throw new NullPointerException( "Parameter '"+paramName
				+"' could not be resolved to an expression list." );
		
		return exprList;
	}
	
	public Set<String> getParamNameSet() {
		return paramMap.keySet();
	}
	
	public int paramMapSize() {
		return paramMap.size();
	}

	@Override
	public List<Expression> substitute(
		Map<String,DefMacro> defMacroMap, Map<String,List<Expression>> substitMap ) {

		List<Expression> result;
		ApplyExpression nApply;
		List<Expression> paramExprSet;
		List<Expression> nParamExprSet;
		
		result = new LinkedList<>();
		nApply = new ApplyExpression();
		result.add( nApply );
		
		for( String param : paramMap.keySet() ) {
			
			paramExprSet = paramMap.get( param );
			nParamExprSet = new LinkedList<>();
			
			for( Expression expr : paramExprSet )
				nParamExprSet.addAll(
					expr.substitute( defMacroMap, substitMap ) );
			
			nApply.addParam( param, nParamExprSet );
		}
		
		return result;
	}
	
	@Override
	public String toString() {
		
		String ret;
		
		ret = "apply(";
		
		for( String param : paramMap.keySet() ) {
			
			ret += " "+param+" : ";
			
			for( Expression expr : paramMap.get( param ) )
				ret += expr+" ";
		}
		
		ret += ")";
		
		return ret;
	}
}
