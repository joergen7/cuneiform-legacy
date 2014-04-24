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

import org.junit.Test;

import de.huberlin.cuneiform.language.Assign;
import de.huberlin.cuneiform.language.CuneiformParser;
import de.huberlin.cuneiform.language.Expression;
import de.huberlin.cuneiform.language.StringExpression;
import de.huberlin.cuneiform.test.common.BaseCuneiformTest;

public class CuneiformTargetTest extends BaseCuneiformTest {

	@SuppressWarnings("static-method")
	@Test
	public void testTargetMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		Assign a;
		StringExpression se;
		
		
		filename = "test/unittest/testTargetMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
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
		
		assertEquals( 2, parser.targetSetSize() );
		assertTrue( parser.containsTarget( "x" ) );
		assertTrue( parser.containsTarget( "y" ) );
	}
	
	// target with no rule must fail
	@SuppressWarnings("static-method")
	@Test
	public void testTargetNoRuleMustFail() throws IOException {

		CuneiformParser parser;
		String filename;
		
		
		filename = "test/unittest/testTargetNoRuleMustFail.cf";

		parser = createParser( filename );
				
		assertTrue( "Parser must return with error", parser.hasError() );
	}

	// duplicate target variable must fail test
	@SuppressWarnings("static-method")
	@Test
	public void testTargetDuplicateVarMustFail() throws IOException {

		CuneiformParser parser;
		String filename;
		
		
		filename = "test/unittest/testTargetDuplicateVarMustFail.cf";

		parser = createParser( filename );
		
		assertTrue( "Parser must return with error", parser.hasError() );
	}
}
