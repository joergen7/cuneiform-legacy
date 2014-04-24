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

import de.huberlin.cuneiform.language.CuneiformParser;
import de.huberlin.cuneiform.language.Expression;
import de.huberlin.cuneiform.language.IdExpression;
import de.huberlin.cuneiform.language.DefMacro;
import de.huberlin.cuneiform.language.StringExpression;
import de.huberlin.cuneiform.test.common.BaseCuneiformTest;


public class CuneiformDefMacroTest extends BaseCuneiformTest {

	// macro must parse test
	@SuppressWarnings("static-method")
	@Test
	public void testDefMacroMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		DefMacro macro;
		StringExpression se;
		IdExpression ie;
		
		filename = "test/unittest/"
			+"testDefMacroMustParse.cf";

		parser = createParser( filename );
				
		assertFalse( "Parser must not return with errors", parser.hasError() );
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
	
	// TODO: What happens if I call a macro that is not defined?
	
	// TODO: What happens if the erasure of the defmacro doesn't fit its call?
	//       Test more parameters and less parameters.
}
