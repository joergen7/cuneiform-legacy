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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.huberlin.cuneiform.language.DefTask;

public class ExecDag extends CuneiformDag {
	
	public ExecDag() {}
	
	public ExecDag( String dagid ) {
		super( dagid );
	}

	private void update() {
		
		boolean update;
		int i;
		CombiTable combiTable;
		DefTask defTaskExample;
		DataList dataList;
		WfElement parent;
		Invocation invocation;
		Resolveable resolveable;
		String taskName;

		do {
			
			update = false;
			
			for( TaskNode taskNode : getRelevantTaskNodeSet() ) {
				
				// skip task nodes that already have invocations
				if( taskNode.hasInvocationList() )
					continue;
				
				try {
					
					// prepare combi table
					combiTable = new CombiTable();
					
					// fetch task definition example
					defTaskExample = taskNode.getDefTaskExample(); // throws
					
					// register task definition parameters
					combiTable.addDefTaskParam( defTaskExample.getParamSet() );
					
					if( defTaskExample.getParamNameSet().size() != taskNode.getParamNameSet().size() )
						throw new RuntimeException(
							"Inconsistent parameter set information. Are there unbound parameters?" );
					
					// register parameter sizes
					for( String paramName : taskNode.getParamNameSet() ) {
						
						parent = taskNode.getParam( paramName );
						dataList = parent.getDataList( 0 );
						combiTable.setSize( paramName, dataList.size() ); // throws
					}
					
					if( !combiTable.isAllSizeKnown() )
						throw new RuntimeException(
							"Cannot enumerate invocations if size information is missing." );
					
					// enumerate invocations
					for( i = 0; i < combiTable.size(); i++ ) {
						
						taskName = taskNode.getTaskItemList().get(
							combiTable.indexOfTaskParam( i ) ).getValue();
						
						invocation = Invocation.createInvocation( taskNode, taskName );
						
						// bind all kind of information if possible
						for( String paramName : defTaskExample.getParamNameSet() ) {
							
							dataList = taskNode.getParam( paramName ).getDataList( 0 );
							
							if( defTaskExample.isParamReduce( paramName ) )
								invocation.bindParam( paramName, dataList );
							else {
								resolveable = dataList.get( combiTable.indexOf( paramName, i ) ); // throws
								invocation.bindParam( paramName, resolveable );
							
							}
						}
						
						taskNode.addInvocation( invocation );
						update = true;


					}					
				}
				catch( NotDerivableException e ) {

					/* caught if
					 *   - task definition example cannot be derived
					 *   - data list size for task node parameter is unknown
					 */
				}

					

			}
				
			
		} while( update );
		

	}
	
	public Invocation getInvocationBySignature( int signature ) {
		
		for( Invocation invoc : getInvocationSet() ) {
		
			try {
				if( invoc.getSignature() == signature )
					return invoc;
			}
			catch( NotDerivableException e ) {
				// ignore invocations without signature
			}
		}
		
		throw new RuntimeException( "An invocation with the signature "+signature+" is not registered." );
	}
	

	
	public Set<Invocation> getInvocationSet() {
		
		Set<Invocation> set;
		
		update();

		set = new HashSet<>();
		
		for( TaskNode taskNode : getRelevantTaskNodeSet() )
			
			set.addAll( taskNode.getInvocationList() );
		
		return set;
	}
	
	public Set<Invocation> getReadyInvocationSet() {
		
		Set<Invocation> set;
		
		set = new HashSet<>();
		
		for( Invocation invocation : getInvocationSet() )
			if( invocation.isReady() )
				set.add( invocation );
		
		return set;
	}
	
	public Set<Invocation> getNonComputedInvocationSet() {
		
		Set<Invocation> set;
				
		set = new HashSet<>();
		
		for( Invocation invocation : getInvocationSet() )
			if( !invocation.isComputed() )
				set.add( invocation );
		
		return set;
	}
	
	public Map<String,List<String>> getComputationResultMap() throws NotDerivableException {
		
		HashMap<String,List<String>> map;
		
		map = new HashMap<>();
		
		for( NamedJunction nj : getTerminalWfElementSet() ) {
			map.put( nj.getJunctionName(), nj.getDataList( 0 ).toStringList() );
		}
		
		return map;
	}
	
	public Set<String> getComputationResultSet() throws NotDerivableException {
		
		Map<String,List<String>> map;
		Set<String> set;
		
		map = getComputationResultMap();
		set = new HashSet<>();
		
		for( String key : map.keySet() )
			set.addAll( map.get( key ) );
		
		return set;
		
	}
}
