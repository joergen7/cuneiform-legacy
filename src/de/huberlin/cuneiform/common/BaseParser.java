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

package de.huberlin.cuneiform.common;

import java.util.LinkedList;
import java.util.List;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.TokenStream;

public abstract class BaseParser extends Parser {

	protected static final String ERROR_UNIQUENESS = "Uniqueness constraint violation";
	protected static final String ERROR_EXISTENCE = "Existence constraint violation";


	private List<String> errorList;
	private List<String> warnList;

	public BaseParser( TokenStream input ) {
		
		super( input );
		
		errorList = new LinkedList<>();
		warnList = new LinkedList<>();

	}
	
	public boolean hasError() {	
		return !errorList.isEmpty() || getNumberOfSyntaxErrors() > 0;
	}
	
	public boolean hasWarn() {
		return !warnList.isEmpty();
	}
	
	protected List<String> getErrorList() {
		return errorList;
	}
	
	protected void reportError( String msg ) {
		
		String s;
		
		if( msg == null )
			throw new NullPointerException( "Error message must not be null." );
		
		if( msg.isEmpty() )
			throw new RuntimeException( "Error message must not be empty." );
		
		s = "[Error] "+msg;
		
		System.err.println( s );
		errorList.add( s );
	}
	
	protected void reportWarn( String msg ) {
		
		String s;
		
		if( msg == null )
			throw new NullPointerException( "Warning message must not be null." );
		
		if( msg.isEmpty() )
			throw new RuntimeException( "Warning message must not be empty." );
		
		s = "[Warning] "+msg;
		
		System.err.println( s );
		warnList.add( s );
	}
}
