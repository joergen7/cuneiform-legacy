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

package de.huberlin.cuneiform.test.dag;

import static org.junit.Assert.*;

import org.junit.Test;

import de.huberlin.cuneiform.dag.CombiTable;
import de.huberlin.cuneiform.dag.ParamItem;
import de.huberlin.cuneiform.dag.Typeable;
import de.huberlin.cuneiform.language.CorrelParam;
import de.huberlin.cuneiform.language.DefTaskParam;

public class CombiTableTest {

	@SuppressWarnings("static-method")
	@Test
	public void testCombiTable() {
		
		CombiTable combiTable;
		DefTaskParam idxCluster, fastqParam;
		
		combiTable = new CombiTable();
		
		idxCluster = new CorrelParam();
		idxCluster.add( new ParamItem( "task", Typeable.TYPE_DEFTASK ) );
		idxCluster.add( new ParamItem( "idx", Typeable.TYPE_STAGE ) );
		
		fastqParam = new CorrelParam();
		fastqParam.add( new ParamItem( "fastq", Typeable.TYPE_STAGE ) );
		
		combiTable.addDefTaskParam( idxCluster );
		combiTable.addDefTaskParam( fastqParam );
		
		combiTable.setSize( "task", 3 );
		combiTable.setSize( "idx", 3 );
		combiTable.setSize( "fastq", 5 );
		
		
	}
	
	@SuppressWarnings("static-method")
	@Test
	public void testInconsistentClusterMustFail() {
		
		CombiTable combiTable;
		DefTaskParam idxCluster, fastqParam;
		
		combiTable = new CombiTable();
		
		idxCluster = new CorrelParam();
		idxCluster.add( new ParamItem( "task", Typeable.TYPE_DEFTASK ) );
		idxCluster.add( new ParamItem( "idx", Typeable.TYPE_STAGE ) );
		
		fastqParam = new CorrelParam();
		fastqParam.add( new ParamItem( "fastq", Typeable.TYPE_STAGE ) );
		
		combiTable.addDefTaskParam( idxCluster );
		combiTable.addDefTaskParam( fastqParam );
		
		combiTable.setSize( "fastq", 5 );

		combiTable.setSize( "task", 3 );
		
		try {
			combiTable.setSize( "idx", 4 );
			fail();
		}
		catch( RuntimeException e ) {
			// expected to be thrown
		}
	}
	
	@SuppressWarnings("static-method")
	@Test
	public void testCombiTableMustEstimateCorrectSize() {
		
		CombiTable combiTable;
		DefTaskParam taskParam, fastqParam;
		
		combiTable = new CombiTable();
		
		taskParam = new CorrelParam();
		taskParam.add( new ParamItem( "task", Typeable.TYPE_DEFTASK ) );
		
		fastqParam = new CorrelParam();
		fastqParam.add( new ParamItem( "fa", Typeable.TYPE_STAGE ) );
		
		combiTable.addDefTaskParam( taskParam );
		combiTable.addDefTaskParam( fastqParam );
		
		combiTable.setSize( "task", 3 );
		combiTable.setSize( "fa", 1 );
		
		assertEquals( 3, combiTable.size() );
		
		
	}
}
