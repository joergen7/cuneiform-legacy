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

import de.huberlin.cuneiform.language.CuneiformParser;
import de.huberlin.cuneiform.language.DefTask;
import de.huberlin.cuneiform.test.common.BaseCuneiformTest;

public class CuneiformLabelTest extends BaseCuneiformTest {

	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		Set<String> memberSet;
		
		filename = "test/unittest/testLabelMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "l1" ) );
		assertTrue( parser.containsLabel( "l2" ) );
		assertTrue( parser.containsLabel( "m" ) );
		
		memberSet = parser.getMemberSetForLabel( "m" );
		assertTrue( memberSet.contains( "l1" ) );
		assertTrue( memberSet.contains( "l2" ) );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelReferToSelfMustFail() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename = "test/unittest/testLabelReferToSelfMustFail.cf";

		parser = createParser( filename );
		assertTrue( "Self reference in labels is not allowed.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelEqDeclareMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/testLabelEqDeclareMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "testLabelEqDeclareMustParse" ) );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelMembersParentMemberMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/testLabelMembersParentMemberMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "a" ) );
		assertTrue( parser.containsLabel( "b" ) );
		assertTrue( parser.containsLabel( "c" ) );
		assertEquals( 2, parser.getMemberSetForLabel( "c" ).size() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelMemberSetIsTransitive() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/testLabelMemberSetMustBeTransitive.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "a" ) );
		assertTrue( parser.containsLabel( "b" ) );
		assertTrue( parser.containsLabel( "c" ) );
		assertEquals( 2, parser.getMemberSetForLabel( "c" ).size() );
	}
	
	// test redefinition of label
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelRedefinitionMustFail() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename = "test/unittest/testLabelRedefinitionMustFail.cf";

		parser = createParser( filename );
		assertTrue( "Label redefinition is not allowed.", parser.hasError() );
	}

	// test reference to undefined label
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelReferToUndefinedMustFail() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename = "test/unittest/testLabelReferToUndefinedMustFail.cf";

		parser = createParser( filename );
		assertTrue( "Reference to undefined label is not allowed.", parser.hasError() );
	}
	
	// test label before declare must fail
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelBeforeDeclareMustFail() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename = "test/unittest/testLabelBeforeDeclareMustFail.cf";

		parser = createParser( filename );
		assertTrue( "Label before declare statement is not allowed.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelDagMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		Set<String> memberSet;
		
		filename = "test/unittest/testLabelDagMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "s" ) );
		
		assertTrue( parser.containsLabel( "l1" ) );
		memberSet = parser.getMemberSetForLabel( "l1" );
		assertTrue( memberSet.contains( "s" ) );
		
		assertTrue( parser.containsLabel( "l2" ) );
		memberSet = parser.getMemberSetForLabel( "l2" );
		assertTrue( memberSet.contains( "s" ) );
		
		assertTrue( parser.containsLabel( "m" ) );
		memberSet = parser.getMemberSetForLabel( "m" );
		assertTrue( memberSet.contains( "l1" ) );
		assertTrue( memberSet.contains( "l2" ) );
	}
	
	// unused label must warn
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelUnusedMustWarn() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/testLabelUnusedMustWarn.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertTrue( "Parser must warn.", parser.hasWarn() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelBashRefMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		DefTask defTask;
		
		filename = "test/unittest/testLabelBashRefMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "bash" ) );
		
		assertTrue( parser.containsDefTask( "my-task" ) );
		defTask = parser.getDefTask( "my-task" );
		assertTrue( defTask.containsLabel( "bash" ) );
	}

	@SuppressWarnings( "static-method" )
	@Test
	public void testLabelBashImplicitMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		DefTask defTask;
		
		filename = "test/unittest/testLabelBashImplicitMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "bash" ) );
		
		assertTrue( parser.containsDefTask( "my-task" ) );
		defTask = parser.getDefTask( "my-task" );
		assertTrue( defTask.containsLabel( "bash" ) );
	}
}
