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

public class DataItem implements Resolveable {
	
	private String value;
	
	public DataItem( String value ) {
		setValue( value );
	}
	
	@Override
	public String getValue() {
		return value;
	}
	
	public void setValue( String value ) {
		
		if( value == null )
			throw new NullPointerException( "Value string must not be null." );
		
		if( value.isEmpty() )
			throw new RuntimeException( "Value string must not be empty." );
		
		this.value = value;
	}

	@Override
	public boolean isInvocation() {
		return false;
	}

	@Override
	public Invocation getInvocation() {
		throw new UnsupportedOperationException( "Operation not supported." );
	}

	@Override
	public Set<Invocation> getInvocationSet() {
		return new HashSet<>();
	}
	
	@Override
	public String toString() {
		return "'"+value+"'";
	}

}
