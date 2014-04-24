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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class Extend {
	
	private Set<String> labelSet;
	private BeforeMethod before;
	private AfterMethod after;
	private ForInputMethod forInput;
	private ForOutputMethod forOutput;
	
	public Extend() {
		labelSet = new HashSet<>();
	}
	
	public void addAuxMethod( BeforeMethod am ) {
		
		if( am == null )
			throw new NullPointerException( "Before method must not be null." );
		
		this.before = am;
	}

	public void addAuxMethod( AfterMethod am ) {
		
		if( am == null )
			throw new NullPointerException( "After method must not be null." );
		
		this.after = am;
	}

	public void addAuxMethod( ForInputMethod am ) {
		
		if( am == null )
			throw new NullPointerException( "For-input method must not be null." );
		
		this.forInput = am;
	}

	public void addAuxMethod( ForOutputMethod am ) {
		
		if( am == null )
			throw new NullPointerException( "For-output method must not be null." );
		
		this.forOutput = am;
	}

	public void addLabel( String label ) {
		
		if( label == null )
			throw new NullPointerException( "Label must not be null." );
		
		labelSet.add( label );
	}
	
	public boolean containsLabel( String label ) {
		return labelSet.contains( label );
	}
	
	/** Finds out whether one of the extension matches a set of labels.
	 * 
	 * Returns true if any of the given labels appears in this extension's
	 * label set.
	 * 
	 * @param labelSet0 A set of labels.
	 * @return True if extensions label set contains any of the given labels.
	 */
	public boolean containsAnyLabel( Collection<String> labelSet0 ) {
		
		for( String label : labelSet0 )
			if( labelSet.contains( label ) )
				return true;
		
		return false;
	}
	
	public AfterMethod getAfter() {
		
		if( after == null )
			throw new NullPointerException( "Extend expression does not have after method." );
		
		return after;
	}
	
	public BeforeMethod getBefore() {

		if( before == null )
			throw new NullPointerException( "Extend expression does not have before method." );
		
		return before;
	}
	
	public ForInputMethod getForInput() {
		
		if( forInput == null )
			throw new NullPointerException( "Extend expression does not have for-input method." );
		
		return forInput;
	}
	
	public ForOutputMethod getForOutput() {
		
		if( forOutput == null )
			throw new NullPointerException( "Extend expression does not have for-output method." );
		
		return forOutput;
	}
	
	public boolean hasBefore() {
		return before != null;
	}
	
	public boolean hasAfter() {
		return after != null;
	}
	
	public boolean hasForInput() {
		return forInput != null;
	}
	
	public boolean hasForOutput() {
		return forOutput != null;
	}
	
	@Override
	public String toString() {
		
		String ret;
		
		ret = "extend";
		
		for( String label : labelSet )
			ret += " "+label;
				
		if( before != null )
			ret += " "+before;
		
		if( after != null )
			ret += " "+after;
		
		if( forInput != null )
			ret += " "+forInput;
		
		if( forOutput != null )
			ret += " "+forOutput;

		return ret;
	}
}
