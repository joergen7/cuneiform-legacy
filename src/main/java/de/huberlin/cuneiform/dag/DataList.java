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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class DataList implements Iterable<DataListContent>, Container {
	
	private List<DataListContent> content;
	
	public DataList() {
		content = new LinkedList<>();
	}
	
	public void add( DataListContent item ) {		
		content.add( item );
	}
	
	public void add( List<? extends DataListContent> list ) {
		
		for( DataListContent item : list )
			add( item );
	}
	
	@Override
	public Resolveable get( int idx ) throws NotDerivableException {
		
		int i;
		TaskReference taskReference;
		
		i = idx;
		
		for( DataListContent item : content ) {
			
			if( item instanceof DataItem ) {
				
				if( i == 0 )
					return ( DataItem )item;
				
				i--;
				
				continue;
			}
			
			if( item instanceof TaskReference ) {
				
				taskReference = ( TaskReference )item;
				
				if( i-taskReference.size() < 0 )
					return taskReference.get( i );
				
				i -= taskReference.size();
				
				continue;
			}
		}
		
		throw new IndexOutOfBoundsException(
			"Index "+idx+" exceeds size of data list." );
	}
	
	public void add( DataList list ) {
		
		for( DataListContent item : list )
			add( item );
	}
	
	public Set<Invocation> getInvocationSet() {
		
		Set<Invocation> set;
		
		set = new HashSet<>();
		for( DataListContent c : content )
			set.addAll( c.getInvocationSet() );
		
		return set;
	}

	public DataItem getRealization() throws NotDerivableException {
		
		for( DataListContent item : content )
			if( item instanceof DataItem )
				return ( DataItem )item;
		
		throw new NotDerivableException( "No realization available in typed item list." );
	}
	
	public boolean isEmpty() {
		return content.isEmpty();
	}
	
	@Override
	public int size() throws NotDerivableException {
		
		int len;
		
		len = 0;
		for( DataListContent item : content )
			
			if( item instanceof Container )
				len += ( ( Container )item ).size();
			else
				len++;
		
		return len;
	}

	@Override
	public Iterator<DataListContent> iterator() {
		return content.iterator();
	}

	public Set<TaskReference> getTaskReferenceSet() {
		
		Set<TaskReference> result;
		
		result = new HashSet<>();
		
		for( DataListContent item : content )
			if( item instanceof TaskReference )
				result.add( ( TaskReference )item );
		
		return result;
	}
	
	@Override
	public String toString() {
		
		String ret;
		
		ret = "[";
		for( DataListContent item : content )
			ret += " "+item.toString();
		ret += " ]";
		
		return ret;
	}
	
	public List<String> toStringList() throws NotDerivableException {
		
		int i;
		LinkedList<String> list;
		
		list = new LinkedList<>();
		
		for( i = 0; i < size(); i++ )
			list.add( get( i ).getValue() );
		
		return list;
		
	}
}
