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

import de.huberlin.cuneiform.dag.ParamItem;
import de.huberlin.cuneiform.language.ApplyExpression;
import de.huberlin.cuneiform.language.Assign;
import de.huberlin.cuneiform.language.CuneiformParser;
import de.huberlin.cuneiform.language.DefTask;
import de.huberlin.cuneiform.language.Expression;
import de.huberlin.cuneiform.language.IdExpression;
import de.huberlin.cuneiform.language.DefTaskOutput;
import de.huberlin.cuneiform.language.DefTaskParam;
import de.huberlin.cuneiform.language.SingleOutput;
import de.huberlin.cuneiform.language.StringExpression;
import de.huberlin.cuneiform.language.CorrelParam;
import de.huberlin.cuneiform.test.common.BaseCuneiformTest;

public class CuneiformAssignTest extends BaseCuneiformTest {

	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignStringMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		Assign a;
		StringExpression se;
		
		filename = "test/unittest/"
			+"testAssignStringMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		
		assertEquals( 1, parser.assignSetSize() );
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
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignIdMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		Assign a;
		StringExpression se;
		IdExpression ie;
		
		filename = "test/unittest/"
			+"testAssignIdMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		
		assertEquals( 2, parser.assignSetSize() );
		assertTrue( parser.containsAssign( "x" ) );
		assertTrue( parser.containsAssign( "y" ) );
		
		a = parser.getAssign( "x" );
		assertEquals( 1, a.exprSetSize() );
		assertEquals( 1, a.varListSize() );
		
		for( Expression e : a.getExprList() ) {
			
			assertTrue( e instanceof StringExpression );
			se = ( StringExpression )e;
			assertEquals( "Hallo Welt", se.getValue() );
		}
		
		a = parser.getAssign( "y" );
		assertEquals( 1, a.exprSetSize() );
		assertEquals( 1, a.varListSize() );
		
		for( Expression e : a.getExprList() ) {
			
			assertTrue( e instanceof IdExpression );
			ie = ( IdExpression )e;
			assertEquals( "x", ie.getValue() );
		}
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignApplyIdMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		DefTask task;
		Set<DefTaskParam> ingrpSet;
		List<DefTaskOutput> outvarList;
		Assign assign;
		ApplyExpression ae;
		List<Expression> es;
		StringExpression se;
		String s;
		DefTaskOutput outVar;
		
		filename = "test/unittest/"
			+"testAssignApplyIdMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertEquals( 1, parser.defTaskMapSize() );
		assertTrue( parser.containsDefTask( "my-task" ) );
		
		task = parser.getDefTask( "my-task" );
		assertTrue( task.getLabelSet().isEmpty() );
		
		ingrpSet = task.getParamSet();
		assertEquals( 1, ingrpSet.size() );
		
		for( DefTaskParam ingrp : ingrpSet ) {
			
			assertEquals( 1, ingrp.size() );
			assertTrue( ingrp instanceof CorrelParam );
			
			for( ParamItem invar : ingrp )
				assertEquals( "x", invar.getValue() );
		}
		
		outvarList = task.getOutputList();
		assertEquals( 1, outvarList.size() );
		outVar = outvarList.get( 0 );
		assertEquals( "f", outVar.getValue() );
		assertTrue( outVar instanceof SingleOutput );
		assertEquals( " cat $x > $f ", parser.getDefTaskBody( "my-task" ) );
		
		assertEquals( 1, parser.assignSetSize() );
		assertTrue( parser.containsAssign( "x" ) );
		assign = parser.getAssign( "x" );
		
		assertEquals( 1, assign.exprSetSize() );
		assertEquals( 1, assign.varListSize() );
		
		for( Expression expr : assign.getExprList() ) {
			
			assertTrue( expr instanceof ApplyExpression );
			
			ae = ( ApplyExpression )expr;
			assertTrue( ae.containsParam( "task" ) );
			es = ae.getExprForParam( "task" );
			assertEquals( 1, es.size() );
			for( Expression e : es ) {
				
				assertTrue( e instanceof IdExpression );
				s = ( ( IdExpression )e ).getValue();
				assertEquals( "my-task", s );
			}

			assertTrue( ae.containsParam( "x" ) );
			es = ae.getExprForParam( "x" );
			assertEquals( 1, es.size() );
			for( Expression e : es ) {
				
				assertTrue( e instanceof StringExpression );
				
				se = ( StringExpression )e;
				assertEquals( "test.txt", se.getValue() );
			}
		}
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignApplyStringMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		DefTask task;
		Set<DefTaskParam> ingrpSet;
		List<DefTaskOutput> outvarList;
		Assign assign;
		ApplyExpression ae;
		List<Expression> es;
		StringExpression se;
		String s;
		DefTaskOutput outVar;
		
		filename = "test/unittest/"
			+"testAssignApplyStringMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertEquals( 1, parser.defTaskMapSize() );
		assertTrue( parser.containsDefTask( "my-task" ) );
		
		task = parser.getDefTask( "my-task" );
		assertTrue( task.getLabelSet().isEmpty() );
		
		ingrpSet = task.getParamSet();
		assertEquals( 1, ingrpSet.size() );
		
		for( DefTaskParam ingrp : ingrpSet ) {
			
			assertEquals( 1, ingrp.size() );
			assertTrue( ingrp instanceof CorrelParam );
			
			for( ParamItem invar : ingrp )
				assertEquals( "x", invar.getValue() );
		}
		
		outvarList = task.getOutputList();
		assertEquals( 1, outvarList.size() );
		outVar = outvarList.get( 0 );
		assertEquals( "f", outVar.getValue() );
		assertTrue( outVar instanceof SingleOutput );
		
		assertEquals( " cat $x > $f ", parser.getDefTaskBody( "my-task" ) );
		
		assertEquals( 1, parser.assignSetSize() );
		assertTrue( parser.containsAssign( "x" ) );
		assign = parser.getAssign( "x" );
		
		assertEquals( 1, assign.exprSetSize() );
		assertEquals( 1, assign.varListSize() );
		
		for( Expression expr : assign.getExprList() ) {
			
			assertTrue( expr instanceof ApplyExpression );
			
			ae = ( ApplyExpression )expr;
			assertTrue( ae.containsParam( "task" ) );
			es = ae.getExprForParam( "task" );
			assertEquals( 1, es.size() );
			for( Expression e : es ) {
				
				assertTrue( e instanceof StringExpression );
				s = ( ( StringExpression )e ).getValue();
				assertEquals( "my-task", s );
			}

			assertTrue( ae.containsParam( "x" ) );
			es = ae.getExprForParam( "x" );
			assertEquals( 1, es.size() );
			for( Expression e : es ) {
				
				assertTrue( e instanceof StringExpression );
				
				se = ( StringExpression )e;
				assertEquals( "test.txt", se.getValue() );
			}
		}
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignMultipleMustParse() throws IOException {

		CuneiformParser parser;
		String filename;
		Assign es;
		StringExpression se;
		IdExpression ie;
		
		filename = "test/unittest/"
			+"testAssignMultipleMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsAssign( "x" ) );
		assertTrue( parser.containsAssign( "y" ) );
		
		es = parser.getAssign( "x" );
		assertEquals( 1, es.exprSetSize() );
		assertEquals( 1, es.varListSize() );
		
		for( Expression e : es.getExprList() ) {
			
			assertTrue( e instanceof StringExpression );
			se = ( StringExpression )e;
			assertEquals( "Hallo", se.getValue() );
		}
		
		es = parser.getAssign( "y" );
		assertEquals( 2, es.exprSetSize() );
		assertEquals( 1, es.varListSize() );
		
		for( Expression e : es.getExprList() ) {
			
			if( e instanceof StringExpression ) {
				
				se = ( StringExpression )e;
				assertEquals( "Welt", se.getValue() );
				continue;
			}
			
			if( e instanceof IdExpression ) {
				
				ie = ( IdExpression )e;
				assertEquals( "x", ie.getValue() );
				continue;
			}
			
			fail( "Expected either string or id expression." );			
		}
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignApplyNoTaskdefMustFail() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testAssignNoTaskdefMustFail.cf";

		parser = createParser( filename );
		
		assertTrue( "Parser must not pass if applied task is not defined.",
			parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignDuplicateMustFail() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testAssignDuplicateMustFail.cf";

		parser = createParser( filename );
		
		assertTrue(
			"Parser must not pass if variable is defined multiple times.",
			parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignApplyUnderdefinedMustFail() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testAssignApplyUnderdefinedMustFail.cf";

		parser = createParser( filename );
		
		assertTrue(
			"Parser must not pass if task call has free parameters.",
			parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignSelfMustFail() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testAssignSelfMustFail.cf";

		parser = createParser( filename );
		
		assertTrue(
			"Parser must not pass if assignment refers to itself.",
			parser.hasError() );
	}
	
	// if a variable is not used, a warning is issued.
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignUnusedMustWarn() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testAssignUnusedMustWarn.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must pass without errors.", parser.hasError() );
		assertTrue( "Parser must warn.", parser.hasWarn() );
	}

	// if variable has no rule, fail
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignNoRuleMustFail() throws IOException {

		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/testAssignNoRuleMustFail.cf";

		parser = createParser( filename );
		
		assertTrue( "Parser must not pass.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignStagelessStringMustParse() throws IOException {
		
		CuneiformParser parser;
		String filename;
		Assign assign;
		List<Expression> exprList;
		StringExpression se;
		
		filename = "test/unittest/"
			+"testAssignStagelessStringMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must pass.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );

		assertTrue( parser.containsAssign( "x" ) );
		assertTrue( parser.containsTarget( "x" ) );
		
		assign = parser.getAssign( "x" );
		exprList = assign.getExprList();
		assertEquals( 1, exprList.size() );
		
		for( Expression e : exprList ) {
			
			assertTrue( e instanceof StringExpression );
			se = ( StringExpression )e;
			assertFalse( se.isStage() );
			assertEquals( "stageless string", se.getValue() );
		}
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignStageMixedStringMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testAssignStageMixedStringMustFail.cf";

		parser = createParser( filename );
		
		assertTrue( "Parser must not pass.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignApplyWrongStageMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testAssignApplyWrongStageMustFail.cf";

		parser = createParser( filename );
		
		assertTrue( "Parser must not pass.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignStageMixedIndirectMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testAssignStageMixedIndirectMustFail.cf";

		parser = createParser( filename );
		
		assertTrue( "Parser must not pass.", parser.hasError() );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testAssignReduceStringMustParse() throws IOException {

		CuneiformParser parser;
		String filename;

		filename = "test/unittest/"
			+"testAssignReduceStringMustParse.cf";

		parser = createParser( filename );
		
		assertFalse( "Parser must not return with errors.", parser.hasError() );
		assertFalse( "Parser must not warn.", parser.hasWarn() );
		
		// TODO: traverse the rest of the graph

	}
	
	// TODO: parameter task must always be defined
	
	@SuppressWarnings("static-method")
	@Test
	public void testAssignApplyNoTaskParamMustFail() throws IOException {
		
		CuneiformParser parser;
		String filename;
		
		filename = "test/unittest/"
			+"testAssignApplyNoTaskParamMustFail.cf";
		
		parser = createParser( filename );
		
		assertTrue( "Parser must report an error.", parser.hasError() );
	}
	
	// TODO: task prototypes must have the same erasure
	
	// TODO: mis-ordered assignments must fail
	
	// TODO: assembly of multiple outputs must parse ( x y = 'x' 'y'; )
	
	// TODO: apply a task that has defined parameters that aren't used does not hurt.
	
	// TODO: test whether a task binding can also be a literal
	
	// TODO: What happens if I call a task that is not defined?
	
	// TODO: Assigning the same output variable multiple times must fail x x = apply( task : my-task );
}
