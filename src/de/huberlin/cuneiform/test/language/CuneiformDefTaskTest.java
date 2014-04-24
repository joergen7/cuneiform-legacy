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

package de.huberlin.cuneiform.test.language;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import de.huberlin.cuneiform.language.ReduceParam;
import de.huberlin.cuneiform.language.CorrelParam;
import de.huberlin.cuneiform.language.CuneiformParser;
import de.huberlin.cuneiform.language.DefTask;
import de.huberlin.cuneiform.language.DefTaskParam;
import de.huberlin.cuneiform.test.common.BaseCuneiformTest;

public class CuneiformDefTaskTest extends BaseCuneiformTest {

	@SuppressWarnings( "static-method" )
	@Test
	public void testDeftaskMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		DefTask deftask;
		Set<DefTaskParam> inParamSet;
		CorrelParam cp;
		ReduceParam cpp;
		
		filename = "test/unittest/"
			+"testDeftaskMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		assertTrue( parser.containsDefTask( "my-task" ) );

		assertEquals( 1, parser.defTaskMapSize() );
		deftask = parser.getDefTask( "my-task" );
		assertTrue( deftask.containsLabel( "a" ) );
		assertTrue( deftask.containsLabel( "b" ) );
		assertTrue( deftask.containsExplicitParamWithName( "c" ) );
		assertTrue( deftask.containsExplicitParamWithName( "d" ) );
		assertTrue( deftask.containsExplicitParamWithName( "e" ) );
		assertTrue( deftask.containsExplicitParamWithName( "f" ) );
		assertTrue( deftask.containsOutputWithName( "x" ) );
		assertTrue( deftask.containsOutputWithName( "y" ) );
		assertEquals( " body of $c task3 $d ",
			parser.getDefTaskBody( "my-task" ) );
		
		inParamSet = deftask.getParamSet();
		
		for( DefTaskParam p : inParamSet ) {
			
			if( p instanceof CorrelParam ) {
				
				cp = ( CorrelParam )p;
				
				if( cp.size() == 1 ) {
					assertTrue( cp.contains( "e" ) );
					continue;
				}
				
				assertEquals( 2, cp.size() );
				assertTrue( cp.contains( "c" ) );
				assertTrue( cp.contains( "d" ) );
				continue;
			}
			
			if( p instanceof ReduceParam ) {
				
				cpp = ( ReduceParam )p;
				assertEquals( "f", cpp.getValue() );
				continue;
			}
		}
		
		System.out.println( deftask );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDeftaskImplicitMacroMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testDefTaskImplicitMacroMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		assertTrue( parser.containsMacro( "my-task" ) );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDeftaskVarDuplicateMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDeftaskVarDuplicateMustFail.cf";

		parser = createParser( filename );

		assertTrue( "Parser must not pass.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDeftaskDuplicateMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDeftaskDuplicateMustFail.cf";

		parser = createParser( filename );

		assertTrue( "Parser must not pass.", parser.hasError() );
	}
	
	// if a task definition is not used, no warning or error is produced
	@SuppressWarnings( "static-method" )
	@Test
	public void testDeftaskUnusedMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDeftaskUnusedMustParse.cf";

		parser = createParser( filename );

		assertFalse( "Parser must not report error.", parser.hasError() );
		assertFalse( "parser must not warn.", parser.hasWarn() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskCorrelTaskMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDefTaskCorrelTaskMustParse.cf";

		parser = createParser( filename );

		assertFalse( "Parser must not report error.", parser.hasError() );
		assertFalse( "parser must warn.", parser.hasWarn() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskReduceTaskMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDefTaskReduceTaskMustFail.cf";

		parser = createParser( filename );

		assertTrue( "Parser must report error.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskMentionTaskMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDefTaskMentionTaskMustParse.cf";

		parser = createParser( filename );

		assertFalse( "Parser must not report error.", parser.hasError() );
		assertFalse( "parser must warn.", parser.hasWarn() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskRefTaskMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDefTaskRefTaskMustFail.cf";

		parser = createParser( filename );

		assertTrue( "Parser must report error.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskNameEqLabelMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDefTaskNameEqLabelMustFail.cf";

		parser = createParser( filename );

		assertTrue( "Parser must report error.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskStagelessMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		DefTask defTask;
		
		filename = "test/unittest/testDefTaskStagelessMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must pass.", parser.hasError() );
		assertFalse( "Parser must pass without warnings.", parser.hasWarn() );
		
		assertTrue( parser.containsDefTask( "my-task" ) );
		
		defTask = parser.getDefTask( "my-task" );
		assertTrue( defTask.containsExplicitParamWithName( "a" ) );
		assertTrue( defTask.containsExplicitParamWithName( "b" ) );
		assertTrue( defTask.containsOutputWithName( "x" ) );
		
		assertFalse( defTask.isParamStage( "a" ) );
		assertFalse( defTask.isParamStage( "b" ) );
		assertFalse( defTask.isOutputStage( "x" ) );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskStageParamTaskMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		DefTask defTask;
		
		filename = "test/unittest/testDefTaskStageParamTaskMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must pass.", parser.hasError() );
		assertFalse( "Parser must pass without warnings.", parser.hasWarn() );
		
		assertTrue( parser.containsDefTask( "my-task" ) );
		
		defTask = parser.getDefTask( "my-task" );
		assertTrue( defTask.containsExplicitParamWithName( "x" ) );		
		assertTrue( defTask.containsExplicitParamWithName( "task" ) );
		assertTrue( defTask.containsOutputWithName( "y" ) );
		assertFalse( defTask.isParamStage( "task" ) );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskDuplicateTaskParamMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDefTaskDuplicateTaskParamMustFail.cf";

		parser = createParser( filename );

		assertTrue( "Parser must report error.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskDuplicateParamMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDefTaskDuplicateParamMustFail.cf";

		parser = createParser( filename );

		assertTrue( "Parser must report error.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskNoPrototypeMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename =
			"test/unittest/"
			+"testDefTaskNoPrototypeMustFail.cf";

		parser = createParser( filename );

		assertTrue( "Parser must report error.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskReduceOutputMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		DefTask defTask;
		
		filename = "test/unittest/testDefTaskReduceOutputMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must pass.", parser.hasError() );
		assertFalse( "Parser must pass without warnings.", parser.hasWarn() );
		
		assertTrue( parser.containsDefTask( "mklist" ) );
		
		defTask = parser.getDefTask( "mklist" );
		assertTrue( defTask.containsOutputWithName( "list" ) );
		assertFalse( defTask.isOutputStage( "list" ) );
		assertTrue( defTask.isOutputReduce( "list" ) );
		
		assertEquals( 1, parser.targetSetSize() );
		for( String s : parser.getTargetSet() )
			assertEquals( "x", s );
		
		// TODO: also approve the apply statement
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskReduceParamMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		DefTask defTask;
		
		filename = "test/unittest/testDefTaskReduceParamMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must pass.", parser.hasError() );
		assertFalse( "Parser must pass without warnings.", parser.hasWarn() );
		
		assertTrue( parser.containsDefTask( "compress" ) );
		
		defTask = parser.getDefTask( "compress" );
		assertTrue( defTask.containsOutputWithName( "tar.bz2" ) );
		assertTrue( defTask.isOutputStage( "tar.bz2" ) );
		assertFalse( defTask.isOutputReduce( "tar.bz2" ) );
		assertTrue( defTask.containsExplicitParamWithName( "file-list" ) );
		assertTrue( defTask.isParamStage( "file-list" ) );
		assertTrue( defTask.isParamReduce( "file-list" ) );
		
		assertEquals( 1, parser.targetSetSize() );
		for( String s : parser.getTargetSet() )
			assertEquals( "archive", s );
		
		// TODO: traverse the rest of the graph
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testDefTaskBodyStartsWithTabMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/testDefTaskBodyStartsWithTabMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must pass.", parser.hasError() );
		assertFalse( "Parser must pass without warnings.", parser.hasWarn() );
		
	}
	
	// TODO: deftask in non-existent must fail (in-label does not exist)
	
	// TODO: deftask declares output parameter but output is never touched must fail
	
	// TODO: deftask declares input parameter but input is never touched must warn
	
	// TODO: deftask without output parameter must fail
}
