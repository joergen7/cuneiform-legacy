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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import de.huberlin.cuneiform.language.AfterMethod;
import de.huberlin.cuneiform.language.BeforeMethod;
import de.huberlin.cuneiform.language.CuneiformParser;
import de.huberlin.cuneiform.language.DefTask;
import de.huberlin.cuneiform.language.Extend;
import de.huberlin.cuneiform.language.ForInputMethod;
import de.huberlin.cuneiform.language.ForOutputMethod;
import de.huberlin.cuneiform.test.common.BaseCuneiformTest;

public class CuneiformExtendTest extends BaseCuneiformTest {
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testExtendMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		List<Extend> extendSet;
		BeforeMethod before;
		AfterMethod after;
		ForInputMethod forInput;
		ForOutputMethod forOutput;
		
		filename = "test/unittest/testExtendMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		
		assertTrue( parser.containsLabel( "my-task" ) );
		assertTrue( parser.containsLabel( "some-other-task" ) );
		
		extendSet = parser.getExtendList();
		assertEquals( 1, extendSet.size() );
		
		for( Extend e : extendSet ) {
			
			assertTrue( e.containsLabel( "my-task" ) );
			assertTrue( e.containsLabel( "some-other-task" ) );
			
			assertTrue( e.hasBefore() );
			before = e.getBefore();
			assertEquals( " some statement concerning $a and $b ", before.getBody() );
			assertTrue( before.containsVar( "a" ) );
			assertTrue( before.containsVar( "b" ) );
			
			assertTrue( e.hasAfter() );
			after = e.getAfter();
			assertEquals( " more to do to $c ", after.getBody() );
			assertTrue( after.containsVar( "c" ) );
			
			assertTrue( e.hasForInput() );
			forInput = e.getForInput();
			assertEquals( " delete $x ", forInput.getBody() );
			assertTrue( forInput.containsVar( "x" ) );
			
			assertTrue( e.hasForOutput() );
			forOutput = e.getForOutput();
			assertEquals( " falsify $y ", forOutput.getBody() );
			assertTrue( forOutput.containsVar( "y" ) );			
		}
	}
	
	// unused extension must warn
	@SuppressWarnings( "static-method" )
	@Test
	public void testExtendUnusedMustWarn() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/testExtendUnusedMustWarn.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
		assertTrue( "Parser must warn.", parser.hasWarn() );		
	}
	
	// extend task must parse
	@SuppressWarnings( "static-method" )
	@Test
	public void testExtendTaskMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		DefTask deftask;
		
		filename = "test/unittest/testExtendTaskMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		
		assertTrue( parser.containsDefTask( "my-task" ) );
		deftask = parser.getDefTask( "my-task" );
		assertTrue( deftask.getParamSet().isEmpty() );
		assertTrue( deftask.getOutputList().isEmpty() );
	}

	// extend non-existent must fail
	@SuppressWarnings( "static-method" )
	@Test
	public void testExtendNonExistentMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/testExtendNonExistentMustFail.cf";

		parser = createParser( filename );
				
		assertTrue( "Parser must not pass.", parser.hasError() );
	}
		
	@SuppressWarnings( "static-method" )
	@Test
	public void testExtendBeforeMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		String body;
		
		filename = "test/unittest/testExtendBeforeMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		
		assertTrue( parser.containsDefTask( "my-task" ) );
		body = parser.getDefTaskBody( "my-task" );
		
		System.out.println( body );
		
		assertTrue( body.indexOf( "echo hello world") >= 0 );
		assertTrue( body.indexOf( "cat $a $b > $x" ) >= 0 );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testExtendAfterMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		String body;
		
		filename = "test/unittest/testExtendAfterMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		
		assertTrue( parser.containsDefTask( "my-task" ) );
		body = parser.getDefTaskBody( "my-task" );
		
		System.out.println( body );
		
		assertTrue( body.indexOf( "echo hello world") >= 0 );
		assertTrue( body.indexOf( "cat $a $b > $x" ) >= 0 );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testExtendForInputMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		String body;
		
		filename = "test/unittest/testExtendForInputMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		
		assertTrue( parser.containsDefTask( "my-task" ) );
		body = parser.getDefTaskBody( "my-task" );
		
		System.out.println( body );
		
		assertTrue( body.indexOf( "cat $a" ) >= 0 );
		assertTrue( body.indexOf( "cat $b" ) >= 0 );
		assertTrue( body.indexOf( "echo $a $b > $x" ) >= 0 );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testExtendForOutputMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		String body;
		
		filename = "test/unittest/testExtendForOutputMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		
		assertTrue( parser.containsDefTask( "my-task" ) );
		body = parser.getDefTaskBody( "my-task" );
		
		System.out.println( body );
		
		assertTrue( body.indexOf( "cat $x" ) >= 0 );
		assertTrue( body.indexOf( "echo $a $b > $x" ) >= 0 );
	}
	
	// TODO: unused before method must warn
	
	// TODO: unused after method must warn
	
	// TODO: test whether resolution of labels and mapping to deftask works
	

}
