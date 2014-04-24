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

import org.antlr.v4.runtime.Token;

import de.huberlin.cuneiform.dag.ParamItem;

public class CorrelParam extends DefTaskParam {

	private static final long serialVersionUID = -3138902881692930514L;

	public CorrelParam() {
		super();
	}
	
	public CorrelParam( Token idToken, int type ) {
		
		super();
		
		String s;
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		s = idToken.getText();
		
		if( s == null )
			throw new NullPointerException( "Id string must not be null." );
		
		if( s.isEmpty() )
			throw new RuntimeException( "Id string must not be empty." );
		
		add( new ParamItem( idToken.getText(), type ) );
	}
	
	public void addParam( Token idToken, int type ) {
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		add( new ParamItem( idToken.getText(), type ) );
	}
	
	@Override
	public String toString() {
		
		String ret;
		boolean comma;
		
		ret = "";
		
		if( size() > 1 )
		ret += "[";
		
		comma = false;
		for( ParamItem item : this ) {
		
			if( comma )
				ret += " ";
			
			comma = true;
			
			if( !item.isStage() )
				ret += "~";
			
			if( item.isDefTask() )
				ret += "`";
			
			ret += item.getValue();
		}
		
		if( size() > 1 )
			ret += "]";
		
		return ret;
	}

}
