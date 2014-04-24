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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;

import de.huberlin.cuneiform.language.ApplyExpression;
import de.huberlin.cuneiform.language.Assign;
import de.huberlin.cuneiform.language.CuneiformLexer;
import de.huberlin.cuneiform.language.CuneiformParser;
import de.huberlin.cuneiform.language.DefTask;
import de.huberlin.cuneiform.language.Expression;
import de.huberlin.cuneiform.language.IdExpression;
import de.huberlin.cuneiform.language.MacroExpression;
import de.huberlin.cuneiform.language.StringExpression;


/** A container for all workflow elements, that constitute a workflow DAG.
 * 
 * It is given one or more Cuneiform files via the addInputFile method. Each
 * Cuneiform file is parsed and the workflow graph is assembled within the
 * CuneiformDag instance.
 * 
 * A CuneiformDag instance can be queried to find out the set of starting or
 * terminal tasks.
 * 
 * @author Jorgen Brandt
 *
 */
public class CuneiformDag implements DotTransformable {
	
	public static final String TASK_TOKEN = "task";
	
	private Set<WfElement> elementSet;
	private Set<NamedJunction> terminalSet;
	private Set<String> wfNameSet;
	private String dagid;

	/** Constructor for the CuneiformDag class.
	 * 
	 * Initializes the initial state of the Dag. No further information is
	 * necessary.
	 */
	public CuneiformDag() {
		this( UUID.randomUUID().toString() );
	}
	
	public CuneiformDag( String dagId ) {
		terminalSet = new HashSet<>();
		elementSet = new HashSet<>();
		wfNameSet = new HashSet<>();
		setDagId( dagId );
		
	}
	
	/** Adds a workflow element to the DAG.
	 * 
	 * Make sure, you add only elements you also connect properly to their
	 * interacting workflow elements. I.e. that all the element's parents know
	 * it to be their child and the element itself knows its parents. The same
	 * must be done for all the workflow element's children.
	 * 
	 * Normally, you will want to fill the DAG via handing a Cuneiform file to
	 * the addInputFile method. The addElement method is part of the public
	 * interface only for unit testing.
	 * 
	 * @param element The workflow element to be added.
	 */
	public void addElement( WfElement element ) {
		
		if( element == null )
			throw new NullPointerException(
				"Workflow element must not be null." );
		
		elementSet.add( element );
	}
	
	public void addInputFile( Collection<String> fileSet ) throws IOException {
		
		if( fileSet == null )
			throw new NullPointerException( "File set must not be null." );
		
		for( String s : fileSet )
			addInputFile( s );
	}
	
	/** Populates the workflow DAG with the content of a Cuneiform source file.
	 * 
	 * The addInputFile method parses the given Cuneiform file and constructs
	 * the workflow DAG from its content.
	 * 
	 * Calling addInputFile multiple times compiles the content of multiple
	 * workflows into the same DAG. The output target set is, thereby, the union
	 * of the output targets of all constituting workflows.
	 * 
	 * @param filename The Cuneiform file to be parsed.
	 * @throws IOException
	 */
	public void addInputFile( String filename ) throws IOException {
		
		ANTLRFileStream stream;
		
		if( filename == null )
			throw new NullPointerException( "Filename must not be null." );
		
		if( filename.isEmpty() )
			throw new RuntimeException( "Filename must not be empty." );

		// parse input file
		stream = new ANTLRFileStream( filename );
		addInputStream( stream );	
	}
	
	public void addInputString( String str ) {
		
		ANTLRInputStream stream;
		
		stream = new ANTLRInputStream( str );
		addInputStream( stream );
	}
	
	public void addReport( Collection<JsonReportEntry> report ) {
		
		if( report == null )
			throw new NullPointerException( "Report collection must not be null." );
		
		for( JsonReportEntry entry : report )
			addReport( entry );
	}
	
	@SuppressWarnings("static-method")
	public void addReport( JsonReportEntry report ) {
		
		if( report == null )
			throw new NullPointerException( "Report entry must not be null." );
		
		System.out.print( report );
	}
	
	/** Declares a named junction to be a terminal element.
	 * 
	 * Make sure, you declare only named junctions to be terminal elements that
	 * already are registered workflow elements.
	 * 
	 * Normally you do not need to declare terminal elements yourself. The
	 * addInputFile method does that for you. The addTerminal method is part of
	 * the public interface only for unit testing.
	 * 
	 * @param element The registered named junction to be declared terminal.
	 */
	public void addTerminal( NamedJunction element ) {
		
		if( element == null )
			throw new NullPointerException(
				"Terminal element must not be null." );
		
		if( !elementSet.contains( element ) )
			throw new RuntimeException(
				"Only registered workflow elements can be declared terminal "
				+"elements." );
		
		terminalSet.add( element );		
	}
	
	/** Retrieves all junctions that are anonymous.
	 * 
	 * @return The set of anonymous junctions.
	 */
	public Set<AnonymousJunction> getAnonymousJunctionSet() {
		
		Set<AnonymousJunction> set;
		
		set = new HashSet<>();
		
		for( WfElement element : elementSet )
			if( element instanceof AnonymousJunction )
				set.add( ( AnonymousJunction )element );
		
		return set;
	}
	
	public String getDagId() {
		return dagid;
	}
	
	/** Retrieves all data nodes.
	 * 
	 * @return The set of data nodes.
	 */
	public Set<DataNode> getDataNodeSet() {
		
		Set<DataNode> set;
		
		set = new HashSet<>();
		
		for( WfElement element : elementSet )
			if( element instanceof DataNode )
				set.add( ( DataNode )element );
		
		return set;
	}
	
	public DefTaskNode getDefTaskNode( String taskName ) {
		
		if( taskName == null )
			throw new NullPointerException( "Task name must not be null." );

		if( taskName.isEmpty() )
			throw new RuntimeException( "Task name must not be empty." );
		
		for( DefTaskNode defTaskNode : getDefTaskNodeSet() )
			if( defTaskNode.getTaskName().equals( taskName ) )
				return defTaskNode;
		
		throw new RuntimeException(
			"Task definition node with name '"+taskName+"' not found." );
	}
	
	public DefTask getDefTask( String taskName ) {
		return getDefTaskNode( taskName ).getDefTask();
	}
	
	public Set<DefTaskNode> getDefTaskNodeSet() {
		
		Set<DefTaskNode> set;
		
		set = new HashSet<>();
		
		for( WfElement element : elementSet )
			if( element instanceof DefTaskNode )
				set.add( ( DefTaskNode )element );
		
		return set;
	}
	
	/** Retrieves the set of registered named and anonymous junctions.
	 * 
	 * @return The set of all junctions.
	 */
	public Set<WfElement> getJunctionSet() {
		
		Set<WfElement> set;
		
		set = new HashSet<>();
		
		for( WfElement element : elementSet )
			if( element instanceof AnonymousJunction
				|| element instanceof NamedJunction )
				set.add( element );
		
		return set;
	}
	
	/** Retrieves the named junction with the given name.
	 * 
	 * If the junction isn't a registered workflow element, an exception is
	 * thrown.
	 * 
	 * @param junctionName The name of the junction to be returned. 
	 * @return The junction with the given name.
	 */
	public NamedJunction getNamedJunction( String junctionName ) {
		
		NamedJunction candidate;
		
		if( junctionName == null )
			throw new NullPointerException( "Junction name must not be null." );
		
		if( junctionName.isEmpty() )
			throw new RuntimeException( "Junction name must not be empty." );
		
		for( WfElement e : elementSet )
			
			if( e instanceof NamedJunction ) {
				
				candidate = ( NamedJunction )e;
				if( candidate.getJunctionName().equals( junctionName ) )
					return candidate;
			}
		
		throw new RuntimeException(
			"Junction with name '"+junctionName+"' not found." );
	}
	
	public WfElement getNamedJunctionOrDefTaskNode( String junctionName ) {
		
		NamedJunction candidate;
		DefTaskNode defTaskNode;
		
		if( junctionName == null )
			throw new NullPointerException( "Junction name must not be null." );
		
		if( junctionName.isEmpty() )
			throw new RuntimeException( "Junction name must not be empty." );
		
		for( WfElement e : elementSet ) {
			
			if( e instanceof NamedJunction ) {
				
				candidate = ( NamedJunction )e;
				if( candidate.getJunctionName().equals( junctionName ) )
					return candidate;
				
				continue;
			}
			
			if( e instanceof DefTaskNode ) {
				
				defTaskNode = ( DefTaskNode )e;
				if( defTaskNode.getTaskName().equals( junctionName ) )
					return defTaskNode;
				
				
			}
		}
		
		throw new RuntimeException(
			"Junction with name '"+junctionName+"' not found." );
	}
	
	/** Retrieves the set of named junctions.
	 * 
	 * @return The set of named junctions.
	 */
	public Set<NamedJunction> getNamedJunctionSet() {
		
		Set<NamedJunction> set;
		
		set = new HashSet<>();
		
		for( WfElement element : elementSet )
			if( element instanceof NamedJunction )
				set.add( ( NamedJunction )element );
		
		return set;
	}
	
	/** Retrieves the task nodes that are involved into deriving the result.
	 * 
	 * @return The set of relevant task nodes.
	 */
	public Set<TaskNode> getRelevantTaskNodeSet() {
		
		Set<TaskNode> set;
		
		set = new HashSet<>();
		
		for( NamedJunction j : terminalSet )
			set.addAll( selectTaskNode( j.getDeepParentSet() ) );
		
		return set;
		
	}
	
	public Set<WfElement> getRelevantWfElementSet() {
		
		Set<WfElement> set;
		
		set = new HashSet<>();
		for( WfElement element : terminalSet ) {
			set.add( element );
			set.addAll( element.getDeepParentSet() );
		}
		
		return set;
	}
	
	/** Retrieves the set of workflow elements that do not have parents.
	 * 
	 * @return The set of orphan workflow elements.
	 */
	public Set<WfElement> getStartWfElementSet() {
		
		Set<WfElement> runSet;
		
		runSet = new HashSet<>();
		
		for( WfElement element : elementSet )
			if( element instanceof DataNode || element instanceof DefTaskNode )
				runSet.add( element );
		
		return runSet;
	}
	
	/** Retrieves the set of task nodes that do not depend on any other tasks.
	 * 
	 * @return The set of independent task nodes.
	 */
	public Set<TaskNode> getStartTaskNodeSet() {
		
		Set<TaskNode> runSet;
		TaskNode taskNode;
		boolean hasParent;
		
		runSet = new HashSet<>();
		
		for( WfElement element : elementSet ) {
			
			if( !( element instanceof TaskNode ) )
				continue;
			
			taskNode = ( TaskNode )element;
			
			hasParent = false;
			for( WfElement seed : taskNode.getParentList() )
				
				if( hasTaskNodeParent( seed ) ) {
					hasParent = true;
					break;
				}
			
			if( !hasParent )
				runSet.add( taskNode );
		}
		
		return runSet;
	}
	
	/** Retrieves all registered task nodes.
	 * 
	 * @return The set of task nodes.
	 */
	public Set<TaskNode> getTaskNodeSet() {
		
		Set<TaskNode> set;
		
		set = new HashSet<>();
		
		for( WfElement element : elementSet )
			if( element instanceof TaskNode )
				set.add( ( TaskNode )element );
		
		return set;
	}
	
	public TaskNode getTaskNode( int taskNodeId ) {
		
		for( WfElement element : elementSet )
			if( element.getId() == taskNodeId ) {
				
				if( !( element instanceof TaskNode ) )
					throw new RuntimeException(
						"The given id matches a "+element.getClass()
						+" but not a TaskNode." );
				
				return ( TaskNode )element;
			}
		
		throw new RuntimeException(
			"No workflow element nor task node with the id "+taskNodeId
			+" exists." );
	}
	
	/** Retrieves the set of named junctions that do not have any children.
	 * 
	 * Note, that a childless workflow element by definition cannot be anything
	 * but a named junction. The reason is that all workflow elements must be,
	 * directly or indirectly, part of an assignment.
	 * 
	 * @return The set of childless named junctions.
	 */
	public Set<NamedJunction> getTerminalWfElementSet() {
		return terminalSet;
	}
	
	/** Retrieves the set of task nodes that no other task nodes depend on. 
	 * 
	 * @return The set of terminal task nodes.
	 */
	public Set<TaskNode> getTerminalTaskNodeSet() {
		
		Set<TaskNode> runSet;
		TaskNode taskNode;
		boolean hasChild;
		
		runSet = new HashSet<>();
		
		for( WfElement element : elementSet ) {
			
			if( !( element instanceof TaskNode ) )
				continue;
			
			taskNode = ( TaskNode )element;
			
			hasChild = false;
			for( WfElement seed : taskNode.getChildSet() )
				
				if( hasTaskNodeChild( seed ) ) {
					hasChild = true;
					break;
				}
			
			if( !hasChild )
				runSet.add( taskNode );
		}
		
		return runSet;
	}
	
	/** Retrieves the complete set of workflow elements.
	 * 
	 * @return The set of all workflow elements.
	 */
	public Set<WfElement> getWfElementSet() {
		return Collections.unmodifiableSet( elementSet );
	}
	
	public Map<String,Set<WfElement>> getWfName2WfElementSetMap() {
		
		Map<String,Set<WfElement>> map;
		Set<WfElement> set;
		
		map = new HashMap<>();
		
		for( String wfName : wfNameSet )
			map.put( wfName, new HashSet<WfElement>() );
		
		for( WfElement element : elementSet ) {
			
			set = map.get( element.getWfName() );
			set.add( element );
		}
		
		return map;
	}
	
	/** The names of all workflows in this DAG.
	 * 
	 * Note, that there can be several workflow names because you can add an
	 * abritrary number of Cuneiform files to a single CuneiformDag instance.
	 * 
	 * @return The set of workflow names.
	 */
	public Set<String> getWfNameSet() {
		return Collections.unmodifiableSet( wfNameSet );
	}
	
	public boolean isRelevant( WfElement element ) {
		return getRelevantWfElementSet().contains( element );
	}
	
	public void setDagId( String dagid ) {
		
		if( dagid == null )
			throw new NullPointerException( "Dag id must not be null." );
		
		if( dagid.isEmpty() )
			throw new RuntimeException( "Dag id must not be empty.");
		
		this.dagid = dagid;
	}
	
	/** Translates the Cuneiform DAG into a dot representation.
	 * 
	 * You can use the dot representation of the DAG to create images of the
	 * workflow. Use the graphviz software package to convert dot scripts into
	 * images.
	 * 
	 * @return A dot representation of the DAG.
	 */
	@Override
	public String toDot() {
		
		StringBuffer buf;
		TaskNode taskNode;
		List<WfElement> parentSet;
		WfElement taskParent;
		
		buf = new StringBuffer();
		
		buf.append( "digraph {\n" );
		
		for( WfElement element : getRelevantWfElementSet() ) {
			
			if( element instanceof DataNode )
				continue;
			
			buf.append( element.getDotNode() ).append( "\n" );
			
			if( element instanceof TaskNode ) {
				
				taskNode = ( TaskNode )element;
				parentSet = taskNode.getNonTaskParentList();
				
				taskParent = taskNode.getTaskParent();
				buf.append( taskParent.getDotId() );
				buf.append( " -> " ).append( element.getDotId() );
				buf.append( "[style=dotted];\n" );
			}
			else
				parentSet = element.getParentList();

			for( WfElement c : parentSet ) {
				
				if( c instanceof DataNode )
					continue;
				
				buf.append( c.getDotId() );
				buf.append( " -> " ).append( element.getDotId() );
				buf.append( ";\n" );				
			}
		
		}
		
		buf.append( "}\n" );
		
		return buf.toString();
	}
	
	/** Tells if there is any task node that depends on a given connectable. 
	 * 
	 * @param seed The connectable to be queried
	 * @return True only if the deep set of children contains any task node.
	 */
	public static boolean hasTaskNodeChild( WfElement seed ) {
		
		Set<WfElement> childSet;
		
		if( seed instanceof TaskNode )
			return true;
		
		childSet = seed.getChildSet();
		
		for( WfElement child : childSet )
			if( hasTaskNodeChild( child ) )
				return true;
		
		return false;
	}
	
	/** Tells whether a given connectable depends on any task node.
	 * 
	 * @param seed The connectable to be queried.
	 * @return True only if the deep set of parents contains any task node.
	 */
	public static boolean hasTaskNodeParent( WfElement seed ) {
		
		if( seed instanceof TaskNode )
			return true;
		
		for( WfElement parent : seed.getParentList() )
			if( hasTaskNodeParent( parent ) )
				return true;
		
		return false;
	}
	
	public static Set<TaskNode> selectTaskNode( Collection<WfElement> original ) {
		
		Set<TaskNode> result;
		
		result = new HashSet<>();
		
		for( WfElement candidate : original )
			if( candidate instanceof TaskNode )
				result.add( ( TaskNode )candidate );
		
		return result;
	}
	
	private void addInputStream( CharStream stream ) {
		
		CuneiformLexer lexer;
		CuneiformParser parser;
		String declare;
		DefTaskNode defTaskNode;
		DefTask defTask;
		
		if( stream == null )
			throw new NullPointerException( "Input string must not be null." );
		
		// parse input file
		lexer = new CuneiformLexer( stream );
		parser = new CuneiformParser( new CommonTokenStream( lexer ) );
		parser.script();
		
		
		if( parser.hasError() )
			throw new RuntimeException( "Parser returned with errors." );
		
		declare = parser.getDeclare();
		if( declare == null )
			throw new NullPointerException( "Declare must not be null." );
		
		wfNameSet.add( declare );
		
		// add junctions for tasks
		for( String deftaskName : parser.getDefTaskNameSet() ) {
			
			if( deftaskName == null )
				throw new NullPointerException(
					"Deftask name must not be null." );
			
			defTask = parser.getDefTask( deftaskName );
			
			// create new deftask junction
			defTaskNode = new DefTaskNode( declare, defTask, parser.getDefTaskBody( deftaskName ) );
			
			// add the DefTaskJunction to the set of workflow elements
			elementSet.add( defTaskNode );
		}
		
		// connect assignments
		for( Assign assign : parser.getAssignList() ) {
			
			if( assign == null )
				throw new NullPointerException(
					"Assignment must not be null." );
			
			resolveAssign( parser, assign );
		}

		// mark workflow targets as terminals
		for( String varName : parser.getTargetSet() ) {
		
			if( varName == null )
				throw new NullPointerException(
					"Target variable name must not be null." );
			
			terminalSet.add( getNamedJunction( varName ) );
		}		
		
	}
	
	private List<WfElement> resolve(
		CuneiformParser parser, List<Expression> exprSet ) {
		
		List<WfElement> wfElementList;
		String declare;
		DataNode dataNode;
		String s;
		ApplyExpression ae;
		TaskNode taskNode;
		AnonymousJunction anonymousJunction;
		List<Expression> applyExprList;
		List<WfElement> parentList;
		StringExpression se;
		
		if( parser == null )
			throw new NullPointerException( "Parser must not be null." );
		
		if( exprSet == null )
			throw new NullPointerException(
				"Expression set must not be null." );
		
		if( exprSet.isEmpty() )
			throw new RuntimeException(
				"Input expression set must not be empty." );
		
		declare = parser.getDeclare();
		
		wfElementList = new LinkedList<>();
		
		for( Expression expr : exprSet ) {
			
			if( expr instanceof IdExpression ) {
				
				s = ( ( IdExpression )expr ).getValue();
				wfElementList.add( getNamedJunctionOrDefTaskNode( s ) );
				
				continue;
			}
			
			if( expr instanceof StringExpression ) {
				
				se = ( StringExpression )expr;
				dataNode = new DataNode( declare, se.getValue(), se.isStage() );
				
				wfElementList.add( dataNode );
				elementSet.add( dataNode );
				
				continue;
			}
			
			if( expr instanceof MacroExpression ) {
				
				applyExprList = new LinkedList<>();
				
				applyExprList.addAll(
					expr.substitute(
						parser.getDefMacroMap(),
						new HashMap<String,List<Expression>>() ) );
				
				return resolve( parser, applyExprList );
			}
			
			if( expr instanceof ApplyExpression ) {
				
				assert !( expr instanceof MacroExpression );
				
				taskNode = new TaskNode( this, declare );

				ae = ( ApplyExpression )expr;
				
				for( String paramName : ae.getParamNameSet() ) {
					
					applyExprList = ae.getExprForParam( paramName );
					
					parentList = resolve( parser, applyExprList );

					if( applyExprList.size() <= 0 )
						throw new RuntimeException(
							"Apply expression set must not be empty." );
					
					if( applyExprList.size() > 1 ) {
						
						anonymousJunction = new AnonymousJunction( declare );
						elementSet.add( anonymousJunction );
					
						taskNode.addParent( anonymousJunction, paramName );
						anonymousJunction.addChild( taskNode );

						
						for( WfElement c : parentList ) {
							c.addChild( anonymousJunction );
							anonymousJunction.addParent( c );
						}
					}
					else
						
						// applyExprSet and parentSet have exactly 1 element.
						
						for( WfElement c : parentList ) {
							taskNode.addParent( c, paramName );	
							c.addChild( taskNode );							
						}
				}
				
				wfElementList.add( taskNode );		
				elementSet.add( taskNode );

				continue;
			}
			
			throw new RuntimeException( "Expression type not recognized." );
		}
		
		if( wfElementList.isEmpty() )
			throw new RuntimeException(
				"If a non-empty expression set was input the corresponding "
				+"workflow element set must not be empty." );
		
		return wfElementList;
	}
	
	private void resolveAssign( CuneiformParser parser, Assign assign ) {
		
		List<Expression> exprSet;
		List<String> varList;
		String varName, declare;
		NamedJunction namedJunction;
		int i;
		Set<TaskNode> taskNodeSet;
		List<WfElement> parentSet;
		
		exprSet = assign.getExprList();
		if( exprSet == null )
			throw new NullPointerException(
				"Expression set must not be null." );
		
		varList = assign.getVarList();
		if( varList.isEmpty() )
			throw new RuntimeException(
				"Assignment right hand variable list must not be empty." );
		
		varName = varList.get( 0 );
		if( varName == null )
			throw new NullPointerException(
				"First variable in assignment must not be null." );
		
		declare = parser.getDeclare();
		if( declare == null )
			throw new NullPointerException( "Declare must not be null." );
		
		// connect main variable
		namedJunction = new NamedJunction( declare, varName, 0 );
		elementSet.add( namedJunction );
		parentSet = resolve( parser, exprSet );
		for( WfElement c : parentSet ) {
			
			if( c == null )
				throw new NullPointerException(
					"Connectable must not be null." );
			
			namedJunction.addParent( c );
			c.addChild( namedJunction );
		}
		
		// connect secondary variables
		taskNodeSet = selectTaskNode( parentSet );
		for( i = 1; i < varList.size(); i++ ) {
			
			varName = varList.get( i );
			if( varName == null )
				throw new NullPointerException(
					"Variable name must not be null." );
			
			namedJunction = new NamedJunction( declare, varName, i );
			elementSet.add( namedJunction );
			
			for( TaskNode taskNode : taskNodeSet ) {
				
				taskNode.addChild( namedJunction, i );
				namedJunction.addParent( taskNode );
			}
		}
	}
	
}
