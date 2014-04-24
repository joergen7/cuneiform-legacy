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
import java.util.List;
import java.util.Set;

import org.junit.Test;

import de.huberlin.cuneiform.language.AfterMethod;
import de.huberlin.cuneiform.language.Assign;
import de.huberlin.cuneiform.language.BeforeMethod;
import de.huberlin.cuneiform.language.CuneiformParser;
import de.huberlin.cuneiform.language.DefTask;
import de.huberlin.cuneiform.language.Expression;
import de.huberlin.cuneiform.language.Extend;
import de.huberlin.cuneiform.language.ForInputMethod;
import de.huberlin.cuneiform.language.ForOutputMethod;
import de.huberlin.cuneiform.language.IdExpression;
import de.huberlin.cuneiform.language.DefMacro;
import de.huberlin.cuneiform.language.StringExpression;
import de.huberlin.cuneiform.test.common.BaseCuneiformTest;

public class CuneiformImportTest extends BaseCuneiformTest {

	// test correct import with labels
	@SuppressWarnings( "static-method" )
	@Test
	public void testImportLabelMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		Set<String> memberSet;
		
		filename = "test/unittest/testImportLabelMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "l1" ) );
		assertTrue( parser.containsLabel( "l2" ) );
		assertTrue( parser.containsLabel( "m" ) );

		memberSet = parser.getMemberSetForLabel( "m" );
		assertTrue( memberSet.contains( "l1" ) );
		assertTrue( memberSet.contains( "l2" ) );

	}
	
	// test correct import with labels
	@SuppressWarnings( "static-method" )
	@Test
	public void testImportTaskdefMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		DefTask deftask;
		
		filename = "test/unittest/testImportDeftaskMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsDefTask( "my-task" ) );
		deftask = parser.getDefTask( "my-task" );
		System.out.println( deftask );
		assertTrue( deftask.containsLabel( "a" ) );
		assertTrue( deftask.containsLabel( "b" ) );
		assertTrue( deftask.containsExplicitParamWithName( "c" ) );
		assertTrue( deftask.containsExplicitParamWithName( "d" ) );
		assertTrue( deftask.containsExplicitParamWithName( "e" ) );
		assertTrue( deftask.containsOutputWithName( "x" ) );
		assertTrue( deftask.containsOutputWithName( "y" ) );
		assertEquals( "body of $c task3 $d", parser.getDefTaskBody( "my-task" ).trim() );

	}
	
	// test import before declaration
	@SuppressWarnings( "static-method" )
	@Test
	public void testImportBeforeDeclareMustFail() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename =
			"test/unittest/testImportBeforeDeclareMustFail.cf";

		parser = createParser( filename );

		assertTrue( "Import before declare must fail.", parser.hasError() );
	}
	
	// test redefinition of stuff in imported script
	@SuppressWarnings( "static-method" )
	@Test
	public void testImportAndLabelRedefinitionMustFail() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename = "test/unittest/testImportBeforeDeclareMustFail.cf";

		parser = createParser( filename );
		assertTrue( "Import before declare must fail.", parser.hasError() );
	}
	
	// test file not there
	@SuppressWarnings( "static-method" )
	@Test
	public void testImportNonExistentFileMustFail() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename = "test/unittest/testImportNonExistentFileMustFail.cf";

		parser = createParser( filename );
		assertTrue( "Non-existent file must fail.", parser.hasError() );
	}	
	
	// test diamond shaped import
	@SuppressWarnings( "static-method" )
	@Test
	public void testImportDiamondMustParse() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename = "test/unittest/testImportDiamondMustParse1.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "contrib1" ) );
		assertTrue( parser.containsLabel( "contrib2a" ) );
		assertTrue( parser.containsLabel( "contrib2b" ) );
		assertTrue( parser.containsLabel( "contrib3" ) );
	}
	
	// test error in imported file
	@SuppressWarnings("static-method")
	@Test
	public void testImportErrorPropagationMustParse() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename =
			"test/unittest/testImportErrorPropagationMustParse.cf";

		parser = createParser( filename );
		
		assertTrue( "Non-existent file must fail.", parser.hasError() );
	}

	// import self must parse
	@SuppressWarnings("static-method")
	@Test
	public void testImportSelfMustParse() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename = "test/unittest/testImportSelfMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Import self must parse.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "a" ) );
	}
	
	// import cyclic must parse
	@SuppressWarnings("static-method")
	@Test
	public void testImportCyclicMustParse() throws IOException {
		
		String filename;
		CuneiformParser parser;
		
		filename =
			"test/unittest/testImportCyclicMustParse1.cf";

		parser = createParser( filename );
		
		assertFalse( "Import cyclic must parse.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsLabel( "a" ) );
		assertTrue( parser.containsLabel( "b" ) );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testImportExtendMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		List<Extend> extendSet;
		BeforeMethod before;
		AfterMethod after;
		ForInputMethod forInput;
		ForOutputMethod forOutput;
		
		filename = "test/unittest/testImportExtendMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		
		assertTrue( parser.containsLabel( "my-task" ) );
		assertTrue( parser.containsLabel( "some-other-task" ) );
		
		extendSet = parser.getExtendList();
		assertEquals( "Wrong number of extend statements.", 1, extendSet.size() );
		
		for( Extend e : extendSet ) {
			
			assertTrue( e.containsLabel( "my-task" ) );
			assertTrue( e.containsLabel( "some-other-task" ) );
			
			before = e.getBefore();
			assertEquals( " some statement concerning $a and $b ", before.getBody() );
			assertTrue( before.containsVar( "a" ) );
			assertTrue( before.containsVar( "b" ) );
			
			after = e.getAfter();
			assertEquals( " more to do to $c ", after.getBody() );
			assertTrue( after.containsVar( "c" ) );
			
			forInput = e.getForInput();
			assertEquals( " delete $x ", forInput.getBody() );
			assertTrue( forInput.containsVar( "x" ) );
			
			forOutput = e.getForOutput();
			assertEquals( " falsify $y ", forOutput.getBody() );
			assertTrue( forOutput.containsVar( "y" ) );			
		}
		

	}

	@SuppressWarnings( "static-method" )
	@Test
	public void testImportAssignMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		Assign a;
		StringExpression se;
		
		filename = "test/unittest/testImportAssignMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsAssign( "x" ) );
		
		a = parser.getAssign( "x" );
		assertEquals( 1, a.exprSetSize() );
		assertEquals( 1, a.varListSize() );
		
		for( Expression e : a.getExprList() ) {
			
			assertTrue( e instanceof StringExpression );
			se = ( StringExpression )e;
			assertEquals( "Hallo Welt", se.getValue() );
		}
	}
	
	// import macro must parse
	@SuppressWarnings( "static-method" )
	@Test
	public void testImportMacroMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		StringExpression se;
		DefMacro macro;
		IdExpression ie;
		
		filename = "test/unittest/testImportMacroMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsMacro( "my-macro" ) );
		
		macro = parser.getMacro( "my-macro" );
		assertTrue( macro.containsVar( "World" ) );
		
		for( Expression e : macro.getExprList() ) {
			
			if( e instanceof StringExpression ) {
				
				se = ( StringExpression )e;
				assertEquals( "Hello", se.getValue() );
				continue;
			}
			
			if( e instanceof IdExpression ) {
				
				ie = ( IdExpression )e;
				assertEquals( "World", ie.getValue() );
				continue;
			}
			
			fail( "Expected String or Id expression." );
		}

	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testImportTargetMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		Assign a;
		StringExpression se;
		
		filename = "test/unittest/testImportTargetMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertEquals( 2, parser.assignSetSize() );
		assertTrue( parser.containsAssign( "x" ) );
		assertTrue( parser.containsAssign( "y" ) );
		
		a = parser.getAssign( "x" );
		assertEquals( 1, a.exprSetSize() );
		for( Expression expr : a.getExprList() ) {
			
			assertTrue( expr instanceof StringExpression );
			se = ( StringExpression )expr;
			assertEquals( "Hallo", se.getValue() );
		}
		
		a = parser.getAssign( "y" );
		assertEquals( 1, a.exprSetSize() );
		for( Expression expr : a.getExprList() ) {
			
			assertTrue( expr instanceof StringExpression );
			se = ( StringExpression )expr;
			assertEquals( "Welt", se.getValue() );
		}
		
		assertEquals( 0, parser.targetSetSize() );
	}
	
	// TODO: What happens, if a referenced file does not exist?
}
