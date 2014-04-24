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

import org.antlr.v4.runtime.Token;

public class MacroExpression extends ApplyExpression {

	private String macroName;
	
	public MacroExpression( String macroName ) {
		setMacroName( macroName );
	}
	
	public MacroExpression( Token idToken ) {
		this( idToken.getText() );
	}
	
	public String getMacroName() {
		return macroName;
	}
	
	public void setMacroName( String macroName ) {
		
		if( macroName == null )
			throw new NullPointerException( "Macro name must not be null." );
		
		if( macroName.isEmpty() )
			throw new RuntimeException( "Macro name must not be empty." );
		
		this.macroName = macroName;
	}

	@Override
	public List<Expression> substitute(
		Map<String,DefMacro> defMacroMap, Map<String,List<Expression>> substitMap ) {

		List<Expression> result;
		Map<String,List<Expression>> nSubstitMap;
		DefMacro defMacro;
		int i, l;
		List<String> varList;
		String key;
		
		if( defMacroMap == null )
			throw new NullPointerException( "Macro definition map must not be null." );
		
		if( substitMap == null )
			throw new NullPointerException( "Substitution map must not be null." );
		
		result = new LinkedList<>();
		
		nSubstitMap = new HashMap<>();
		nSubstitMap.putAll( substitMap );
		
		defMacro = defMacroMap.get( macroName );
		if( defMacro == null )
			throw new NullPointerException(
				"Definition for macro with the name '"+macroName
				+"' not found." );
		
		varList = defMacro.getVarList();
		l = varList.size();
		if( l != paramMapSize() )
			throw new RuntimeException(
				"Length of parameter list does not match macro prototype." );
		
		for( i = 0; i < l; i++ ) {
			
			key = varList.get( i );
			if( nSubstitMap.containsKey( key ) )
				nSubstitMap.put( key, nSubstitMap.get( key ) );
			else
				nSubstitMap.put( key, getExprForParam( key ) );
		}
		
		for( Expression expr : defMacro.getExprList() ) {
			
			if( expr == null )
				throw new NullPointerException( "Expression must not be null." );
			
			List<Expression> subst = expr.substitute( defMacroMap, nSubstitMap );
			
			result.addAll( subst );
		}

		
		return result;
	}
	
	@Override
	public String toString() {
		
		String ret;
		
		ret = macroName+"( ";
		
		for( String param : getParamNameSet() )
			ret += param+" : "+getExprForParam( param ).toString()+" ";
		
		ret += ")";
		
		return ret;
	}
	
	
}
