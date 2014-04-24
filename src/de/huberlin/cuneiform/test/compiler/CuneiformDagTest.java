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

package de.huberlin.cuneiform.test.compiler;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import de.huberlin.cuneiform.dag.CuneiformDag;
import de.huberlin.cuneiform.dag.DataNode;
import de.huberlin.cuneiform.dag.NamedJunction;
import de.huberlin.cuneiform.dag.WfElement;
import de.huberlin.cuneiform.test.common.BaseCuneiformTest;

public class CuneiformDagTest extends BaseCuneiformTest {

	@SuppressWarnings( "static-method" )
	@Test
	public void testWorkflow01() throws IOException {
		
		String filename;
		CuneiformDag dag;
		
		filename = "src/de/huberlin/cuneiform/test/compiler/testCuneiformDag01.cf";
		
		dag = new CuneiformDag();
		dag.addInputFile( filename );
				
		System.out.println( dag.toDot() );
		
		// TODO: Check whether start and terminal sets produced by CuneiformDag
		//       match. Do the same in all other tests
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testWorkflow02() throws IOException {
		
		String filename;
		CuneiformDag dag;
		
		filename = "src/de/huberlin/cuneiform/test/compiler/testCuneiformDag02.cf";
		
		dag = new CuneiformDag();
		dag.addInputFile( filename );
		
		System.out.println( dag.toDot() );
	}

	
	/* @SuppressWarnings( "static-method" )
	@Test
	public void testGetStartTaskNodeSet() {
		
		CuneiformDag cuneiformDag;
		DataNode fa, fastq1, fastq2;
		NamedJunction chr22, caco, merged;
		DefTaskNode bowtieAlign, samtoolsView, samtoolsSort, samtoolsMerge;
		TaskNode task1, task2, task3, task4;
		String workflowName;
		Set<WfElement> startWfElementSet;
		Set<TaskNode> startTaskNodeSet;
		
		
		cuneiformDag = new CuneiformDag();
		workflowName = "testGetStartTaskNodeSet";
		
		fa = new DataNode( workflowName, "example_workflow/chr22", false );
		cuneiformDag.addElement( fa );
		
		chr22 = new NamedJunction( workflowName, "chr22" );
		chr22.addParent( fa );
		fa.addChild( chr22 );
		cuneiformDag.addElement( chr22 );
		
		fastq1 = new DataNode( workflowName, "reads/caco.1.0001.fastq", true );
		cuneiformDag.addElement( fastq1 );
		
		fastq2 = new DataNode( workflowName, "reads/caco.1.0002.fastq", true );
		cuneiformDag.addElement( fastq2 );
		
		caco = new NamedJunction( workflowName, "caco" );
		caco.addParent( fastq1 );
		fastq1.addChild( caco );
		caco.addParent( fastq2 );
		fastq2.addChild( caco );
		cuneiformDag.addElement( caco );
		
		// bowtieAlign
		bowtieAlign = new DefTaskNode( workflowName, "bowtieAlign" );
		cuneiformDag.addElement( bowtieAlign );
		
		// task1
		task1 = new TaskNode( cuneiformDag, workflowName );
		cuneiformDag.addElement( task1 );
		
		bowtieAlign.addChild( task1 );
		task1.addParent( bowtieAlign, "task" );
		
		chr22.addChild( task1 );
		task1.addParent( chr22, "idx" );
		
		caco.addChild( task1 );
		task1.addParent( caco, "fastq" );
		
		// samtoolsView
		samtoolsView = new DefTaskNode( workflowName, "samtoolsView" );
		cuneiformDag.addElement( samtoolsView );
		
		// task2
		task2 = new TaskNode( cuneiformDag, workflowName );
		cuneiformDag.addElement( task2 );
		
		samtoolsView.addChild( task2 );
		task2.addParent( samtoolsView, "task" );
		
		task1.addChild( task2 );
		task2.addParent( task1, "sam" );
		
		// samtoolsSort
		samtoolsSort = new DefTaskNode( workflowName, "samtoolsSort" );
		cuneiformDag.addElement( samtoolsSort );
		
		// task3
		task3 = new TaskNode( cuneiformDag, workflowName );
		cuneiformDag.addElement( task3 );
		
		samtoolsSort.addChild( task3 );
		task3.addParent( samtoolsSort, "task" );
		
		task2.addChild( task3 );
		task3.addParent( task2, "bam" );
		
		// samtoolsMerge
		samtoolsMerge = new DefTaskNode( workflowName, "samtoolsMerge" );
		cuneiformDag.addElement( samtoolsMerge );
		
		// task4
		task4 = new TaskNode( cuneiformDag, workflowName );
		cuneiformDag.addElement( task4 );
		
		
		samtoolsMerge.addChild( task4 );
		task4.addParent( samtoolsMerge, "task" );
		
		task3.addChild( task4 );
		task4.addParent( task3, "bam" );
		
		//merged
		merged = new NamedJunction( workflowName, "merged" );
		cuneiformDag.addElement( merged );
		cuneiformDag.addTerminal( merged );

		merged.addParent( task4 );
		task4.addChild( merged );

		
		System.out.println( cuneiformDag.toDot() );
		
		startWfElementSet = cuneiformDag.getStartWfElementSet();
		assertEquals( 7, startWfElementSet.size() );
		assertTrue( startWfElementSet.contains( fa ) );
		assertTrue( startWfElementSet.contains( fastq1 ) );
		assertTrue( startWfElementSet.contains( fastq2 ) );
		assertTrue( startWfElementSet.contains( bowtieAlign ) );
		assertTrue( startWfElementSet.contains( samtoolsView ) );
		assertTrue( startWfElementSet.contains( samtoolsSort ) );
		assertTrue( startWfElementSet.contains( samtoolsMerge ) );
		
		
		startTaskNodeSet = cuneiformDag.getStartTaskNodeSet();
		
		assertFalse( startTaskNodeSet.contains( task2 ) );
		assertFalse( startTaskNodeSet.contains( task3 ) );
		assertFalse( startTaskNodeSet.contains( task4 ) );
		assertTrue( startTaskNodeSet.contains( task1 ) );
		assertEquals( 1, startTaskNodeSet.size() );
	} */
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testMacroIdReplaceMustParse() throws IOException {
		
		String filename;
		CuneiformDag dag;
		NamedJunction a, b, c;
		List<WfElement> parentSet;
		Set<WfElement> childSet;
		
		filename = "src/de/huberlin/cuneiform/test/compiler/testMacroIdReplaceMustParse.cf";
		
		dag = new CuneiformDag();
		dag.addInputFile( filename );
		
		System.out.println( dag.toDot() );

		a = dag.getNamedJunction( "a" );
		b = dag.getNamedJunction( "b" );
		c = dag.getNamedJunction( "c" );

		assertTrue( a.getChildSet().isEmpty() );
		parentSet = a.getParentList();
		assertEquals( 1, parentSet.size() );
		for( WfElement parent : parentSet )
			assertTrue( parent instanceof DataNode );
		
		childSet = b.getChildSet();
		assertEquals( 1, childSet.size() );
		for( WfElement child : childSet )
			assertEquals( c, child );
		parentSet = b.getParentList();
		assertEquals( 1, parentSet.size() );
		for( WfElement parent : parentSet )
			assertTrue( parent instanceof DataNode );
				
		assertTrue( c.getChildSet().isEmpty() );
		parentSet = c.getParentList();
		assertEquals( 1, parentSet.size() );
		for( WfElement parent : parentSet )
			assertEquals( b, parent );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testMacroIdAugmentMustParse() throws IOException {
		
		String filename;
		CuneiformDag dag;
		NamedJunction a, b, c;
		List<WfElement> parentSet;
		Set<WfElement> childSet;
		
		filename = "src/de/huberlin/cuneiform/test/compiler/testMacroIdAugmentMustParse.cf";
		
		dag = new CuneiformDag();
		dag.addInputFile( filename );
		
		System.out.println( dag.toDot() );

		a = dag.getNamedJunction( "a" );
		b = dag.getNamedJunction( "b" );
		c = dag.getNamedJunction( "c" );

		childSet = a.getChildSet();
		assertEquals( 1, childSet.size() );
		for( WfElement child : childSet )
			assertEquals( c, child );
		parentSet = a.getParentList();
		assertEquals( 1, parentSet.size() );
		for( WfElement parent : parentSet )
			assertTrue( parent instanceof DataNode );
		
		childSet = b.getChildSet();
		assertEquals( 1, childSet.size() );
		for( WfElement child : childSet )
			assertEquals( c, child );
		parentSet = b.getParentList();
		assertEquals( 1, parentSet.size() );
		for( WfElement parent : parentSet )
			assertTrue( parent instanceof DataNode );
				
		assertTrue( c.getChildSet().isEmpty() );
		parentSet = c.getParentList();
		assertEquals( 2, parentSet.size() );
		for( WfElement parent : parentSet )
			assertTrue( b == parent || a == parent );
	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testMacroApplyMustParse() throws IOException {
		
		String filename;
		CuneiformDag dag;
		
		filename = "src/de/huberlin/cuneiform/test/compiler/testMacroApplyMustParse.cf";
		
		dag = new CuneiformDag();
		dag.addInputFile( filename );
		
		System.out.println( dag.toDot() );

		// TODO: check for everything that is supposed to be there

	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testMacroCallsMacroMustParse() throws IOException {
		
		String filename;
		CuneiformDag dag;
		
		filename = "src/de/huberlin/cuneiform/test/compiler/testMacroCallsMacroMustParse.cf";
		
		dag = new CuneiformDag();
		dag.addInputFile( filename );
		
		System.out.println( dag.toDot() );

		// TODO: check for everything that is supposed to be there

	}
	
	@SuppressWarnings( "static-method" )
	@Test
	public void testMacroNestedMustParse() throws IOException {
		
		String filename;
		CuneiformDag dag;
		
		filename = "src/de/huberlin/cuneiform/test/compiler/testMacroNestedMustParse.cf";
		
		dag = new CuneiformDag();
		dag.addInputFile( filename );
		
		System.out.println( dag.toDot() );

		// TODO: check for everything that is supposed to be there

	}
}
