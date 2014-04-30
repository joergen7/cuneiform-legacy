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

public class RawBodyContainer {

	private String body;

	public RawBodyContainer() {
		setBody( "*{}*" );
	}
	
	public void setBody( Token bodyToken ) {
		
		if( bodyToken == null )
			throw new NullPointerException( "Body token must not be null." );
		
		setBody( bodyToken.getText() );
	}
	
	public void setBody( String body ) {
		
		if( body == null )
			throw new NullPointerException( "Body must not be null." );
		
		if( body.length() < 4 )
			throw new RuntimeException( "Body length must at least be 4." );
		
		if( !body.startsWith( "*{" ) )
			throw new RuntimeException( "Body must start with '*{'." );
		
		if( !body.endsWith( "}*" ) )
			throw new RuntimeException( "Body must end with '}*'." );
		
		this.body = body.substring( 2, body.length()-2 ).replace( "\r", "" );
	}
	
	public String getBody() {
		return body;
	}	

}
