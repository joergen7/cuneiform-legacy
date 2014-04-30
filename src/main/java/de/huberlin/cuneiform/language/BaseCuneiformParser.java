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

package de.huberlin.cuneiform.language;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;

import de.huberlin.cuneiform.common.BaseParser;
import de.huberlin.cuneiform.common.Constant;
import de.huberlin.cuneiform.common.ForeignLangCatalog;
import de.huberlin.cuneiform.dag.ParamItem;

public abstract class BaseCuneiformParser extends BaseParser {
	
	protected static final String ERROR_ORDER = "Order constraint violation";
	protected static final String ERROR_REFERENCE = "Reference constraint violation";
	protected static final String ERROR_IO = "Input/Output error";
	protected static final String ERROR_IMPORT = "Import error";
	protected static final String ERROR_BROKEN_INSTALL = "Broken installation";
	protected static final String WARN_UNUSED = "Unused expression";

	private String declare;
	private Map<String,Set<String>> labelMap;
	private Set<String> blackList;
	private Map<String,DefTask> defTaskMap;
	private List<Extend> extendList;
	private Extend curExtend;
	private Set<String> targetSet;
	private Assign curAssign;
	private List<Assign> assignList;
	private DefMacro curDefMacro;
	private Map<String,DefMacro> defMacroMap;
	
	public BaseCuneiformParser( TokenStream input ) {
		
		super( input );
		
		labelMap = new HashMap<>();
		blackList = new HashSet<>();
		defTaskMap = new HashMap<>();
		extendList =  new LinkedList<>();
		targetSet = new HashSet<>();
		assignList = new LinkedList<>();
		defMacroMap = new HashMap<>();
		
		for( String langLabel : ForeignLangCatalog.getLangLabelArray() )
			labelMap.put( langLabel, new HashSet<String>() );
	}
	
	public int assignSetSize() {
		return assignList.size();
	}
	
	public boolean containsAssign( String varname ) {
		
		for( Assign a : assignList )
			if( a.containsVar( varname ) )
				return true;
		
		return false;
	}
	
	public boolean containsDefTask( String name ) {
		
		if( name == null )
			throw new NullPointerException( "Task name must not be null." );
		
		if( name.isEmpty() )
			throw new RuntimeException( "Task name must not be empty." );

		return defTaskMap.containsKey( name );
	}
	
	public boolean containsLabel( String label ) {
		
		if( label == null )
			throw new NullPointerException( "Label must not be null." );
		
		if( label.isEmpty() )
			throw new RuntimeException( "Label must not be empty." );

		return labelMap.containsKey( label );
	}
	
	public boolean containsMacro( String name ) {
		
		if( name == null )
			throw new NullPointerException( "Macro name must not be null." );
		
		if( name.isEmpty() )
			throw new RuntimeException( "Macro name must not be empty." );
		
		return defMacroMap.containsKey( name );
	}
	
	public boolean containsTarget( String name ) {
		
		if( name == null )
			throw new NullPointerException( "Target variable name must not be null." );
		
		if( name.isEmpty() )
			throw new RuntimeException( "Target variable name must not be empty." );
		
		return targetSet.contains( name );
	}
	
	public int defTaskMapSize() {
		return defTaskMap.size();
	}
	
	public String getDeclare() {
		
		if( declare == null )
			throw new NullPointerException( "Declare attribute was never set." );
		
		return declare;
	}
	
	public Assign getAssign( String varname ) {
		
		for( Assign a : assignList )
			if( a.containsVar( varname ) )
				return a;
		
		throw new RuntimeException( "No assignment for variable '"+varname+"' found." );
	}
	
	public List<Assign> getAssignList() {
		return Collections.unmodifiableList( assignList );
	}
	
	public Map<String,DefMacro> getDefMacroMap() {
		return defMacroMap;
	}
	
	public DefTask getDefTask( String taskname ) {
		
		DefTask result;
		
		if( taskname == null )
			throw new NullPointerException( "Task name must not be null." );
		
		if( taskname.isEmpty() )
			throw new RuntimeException( "Task name must not be empty." );
		
		result = defTaskMap.get( taskname );
		if( result == null )
			throw new NullPointerException(
				"A task with the specified name '"+taskname
				+"' does not exist." );
		
		return result;
	}
	
	/** Returns the body of a task definition. All extensions included.
	 * 
	 * @param taskName The name of the task for which to retrieve the body.
	 * @return The body of the task.
	 */
	public String getDefTaskBody( String taskName ) {
		
		String ret;
		Set<String> labelSet;
		DefTask defTask;
		BeforeMethod before;
		AfterMethod after;
		ForInputMethod forInput;
		ForOutputMethod forOutput;
		String var;
		
		defTask = defTaskMap.get( taskName );
		
		ret = defTask.getBody();
		
		labelSet = this.getLabelSetForTask( taskName );
		
		
		for( Extend extend : extendList ) {
			
			if( !extend.containsAnyLabel( labelSet ) )
				continue;
			
			// process before
			if( extend.hasBefore() ) {
				
				before = extend.getBefore();
				if( before.varSetSubsetOf( defTask.getParamNameSet() ) )
					ret = before.getBody()+"\n\n"+ret;
			}
			
			// process after
			if( extend.hasAfter() ) {
				
				after = extend.getAfter();
				if( after.varSetSubsetOf( defTask.getOutputNameList() ) )
					ret += "\n\n"+after.getBody();
			}
			
			// process for-input
			if( extend.hasForInput() ) {
				
				forInput = extend.getForInput();
				var = forInput.getVar();
				for( DefTaskParam param : defTask.getParamSet() )
					for( ParamItem paramName : param )
						ret = substitute( forInput.getBody(), var, paramName.getValue() )+"\n\n"+ret;
			}
			
			// process for-output
			if( extend.hasForOutput() ) {
				
				forOutput = extend.getForOutput();
				var = forOutput.getVar();
				for( DefTaskOutput output : defTask.getOutputList() )
					ret += "\n\n"+substitute( forOutput.getBody(), var, output.getValue() );
			}
		}
		
		return ret;
	}
	
	public Collection<DefTask> getDefTaskSet() {		
		return defTaskMap.values();
	}
	
	public Set<String> getDefTaskNameSet() {
		return defTaskMap.keySet();
	}
	
	public List<Extend> getExtendList() {
		
		List<Extend> set;
		
		set = Collections.unmodifiableList( extendList );
		if( set == null )
			throw new NullPointerException( "Extend set must not be null." );
		
		return set;
	}
	
	public Set<String> getLabelSetForTask( String name ) {
		
		DefTask task;
		Set<String> result;
		
		result = new HashSet<>();
		result.add( name );
		
		task = defTaskMap.get( name );
		if( task == null )
			throw new NullPointerException(
				"Task '"+name+"' is not a member of the task set." );
		
		for( String label : task.getLabelSet() ) {
			
			result.add( label );
			result.addAll( getMemberSetForLabel( label ) );
		}
		
		return result;
	}
	
	public DefMacro getMacro( String name ) {
		
		DefMacro macro;
		
		if( name == null )
			throw new NullPointerException( "Macro name must not be null." );
		
		if( name.isEmpty() )
			throw new RuntimeException( "Macro name must not be empty." );
		
		macro = defMacroMap.get( name );
		
		if( macro == null )
			throw new NullPointerException( "Macro '"+name
				+"' is not member of the macro set." );
		
		return macro;
	}
	
	public Set<String> getMemberSetForLabel( String label ) {
		
		Set<String> memberSet;
		Set<String> childSet;
		
		if( label == null )
			throw new NullPointerException( "Label must not be null." );
		
		if( label.isEmpty() )
			throw new RuntimeException( "Label must not be empty." );
		
		memberSet = new HashSet<>();
		childSet = labelMap.get( label );
		if( childSet == null )
			throw new NullPointerException(
				"Label '"+label+"' is not a member of the label set." );
		
		memberSet.addAll( childSet );
		
		for( String s : childSet )
			memberSet.addAll( getMemberSetForLabel( s ) );
		
		return memberSet;
	}
	
	public Set<String> getTargetSet() {
		
		Set<String> set;
		
		set = Collections.unmodifiableSet( targetSet );
		if( set == null )
			throw new NullPointerException( "Target set must not be null." );
		
		return set;
	}
	
	public int targetSetSize() {
		return targetSet.size();
	}
	

	public static String substitute( String orig, String pattern, String replacement ) {
		
		String ret;
		String ret0;
		int i;
		
		ret = orig;
		
		while( ( i = ret.indexOf( "$"+pattern ) ) >= 0 ) {
			ret0 = ret.substring( 0, i+1 )+replacement+ret.substring( i+pattern.length()+1 );
			ret = ret0;
		}
		
		return ret;
	}
	
	protected void addAssign() {
		
		curAssign = new Assign();
		assignList.add( curAssign );
	}
	
	protected void addAssign( List<Assign> list ) {
		
		if( list == null )
			throw new NullPointerException( "Assignment list must not be null." );
		
		assignList.addAll( list );
	}
	
	protected void addAssignExpression( List<Expression> exprSet ) {
		
		if( exprSet == null )
			throw new NullPointerException(
				"Expression set must not be null." );
		
		if( exprSet.isEmpty() )
			throw new RuntimeException( "Expression set must not be emtpy." );
		
		curAssign.addExpression( exprSet );
	}
	
	protected void addAssignVar( Token idToken ) {
		
		String id;
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		id = idToken.getText();
		
		if( id == null )
			throw new NullPointerException( "Id string must not be null." );
		
		if( id.isEmpty() )
			throw new RuntimeException( "Id string must not be empty." );
		
		curAssign.addVar( id );
	}
	
	protected void addAuxMethod( BeforeMethod am ) {
		
		if( am == null )
			throw new NullPointerException( "Before method must not be null." );
		
		if( curExtend.hasBefore() ) {
			
			reportError( ERROR_UNIQUENESS, am.getLine(), "Duplicate definition of before-auxiliary method." );
			return;
		}
		
		curExtend.addAuxMethod( am );
	}
	
	protected void addAuxMethod( AfterMethod am ) {

		if( am == null )
			throw new NullPointerException( "After method must not be null." );
		
		if( curExtend.hasAfter() ) {
			
			reportError( ERROR_UNIQUENESS, am.getLine(), "Duplicate definition of after-auxiliary method." );
			return;
		}
		
		curExtend.addAuxMethod( am );
	}
	
	protected void addAuxMethod( ForInputMethod am ) {
		
		if( am == null )
			throw new NullPointerException( "For-input method must not be null." );
		


		if( curExtend.hasForInput() ) {
			
			reportError( ERROR_UNIQUENESS, am.getLine(), "Duplicate definition of for-input-auxiliary method." );
			return;
		}
		
		curExtend.addAuxMethod( am );
	}
	
	protected void addAuxMethod( ForOutputMethod am ) {
		
		if( am == null )
			throw new NullPointerException( "For-output method must not be null." );
		


		if( curExtend.hasForOutput() ) {
			
			reportError( ERROR_UNIQUENESS, am.getLine(), "Duplicate definition of for-output-auxiliary method." );
			return;
		}
		
		curExtend.addAuxMethod( am );
	}
	
	protected void addBlackListItem( Set<String> bl ) {
		
		if( bl == null )
			throw new NullPointerException( "Black list must not be null." );
		
		blackList.addAll( bl );
	}
	
	protected void addDefMacro( Map<String,DefMacro> map ) {

		if( map == null )
			throw new NullPointerException(
				"Macro definition map must not be null." );
		
		defMacroMap.putAll( map );
	}
	
	protected void addDefTask( Token idToken ) {
		
		String name;
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		name = idToken.getText();
		
		// check whether we have seen a declare statement yet
		if( declare == null ) {
			
			reportError( ERROR_ORDER, idToken.getLine(), "Task definition for '"
				+name+"' must not appear prior to declare statement." );
			
			return;
		}
		
		// check whether this is the first time, this task is introduced
		if( defTaskMap.containsKey( name ) ) {
			
			reportError( ERROR_UNIQUENESS, idToken.getLine(),
				"Duplicate task definition. The task '"+name
				+"' has already been defined." );
			
			return;
		}
		
		defTaskMap.put( name, new DefTask( name ) );
	}
	
	protected void addDefTask( Map<String,DefTask> map ) {

		if( map == null )
			throw new NullPointerException(
				"Task definition map must not be null." );
		
		defTaskMap.putAll( map );
	}
	
	protected void addDefTaskParam( Token nameToken, DefTaskParam p ) {
		
		String name;
		DefTask task;
		
		if( nameToken == null )
			throw new NullPointerException(
				"Task name token must not be null." );
		
		if( p == null )
			throw new NullPointerException(
				"Input parameter must not be null." );
		
		name = nameToken.getText();
		
		task = defTaskMap.get( name );
		if( task == null )
			throw new NullPointerException( "Referenced task does not exits." );

		for( ParamItem invar : p )
			if( task.containsExplicitParamWithName( invar.getValue() ) )
				if( invar.getValue().equals( Constant.TOKEN_TASK ) && task.isTaskParamExplicit() ) {
				
					reportError( BaseParser.ERROR_UNIQUENESS, nameToken.getLine(),
						"Duplicate definition of input variable name '"+invar
						+"'." );
					
					return;
				}
		
		// if this cluster contains the task parameter, it must not be of type reduce
		if( p instanceof ReduceParam )
			if( ( ( ReduceParam )p ).getValue().equals( Constant.TOKEN_TASK ) ) {
				
				reportError( ERROR_REFERENCE, nameToken.getLine(),
					"Special task parameter cannot be marked reduce." );
				
				return;
			}

		task.addParam( p );
	}
	
	protected void addDefTaskLabel( Token tasknameToken, Token labelToken ) {
		
		String taskname;
		String label;
		DefTask task;
		
		if( tasknameToken == null )
			throw new NullPointerException(
				"Task name token must not be null." );
		
		if( labelToken == null )
			throw new NullPointerException( "Label token must not be null." );
		
		taskname = tasknameToken.getText();
		label = labelToken.getText();
		
		task = defTaskMap.get( taskname );
		if( task == null )
			throw new NullPointerException( "Referenced task does not exits." );
		
		if( !labelMap.containsKey( label ) ) {
			
			reportError( ERROR_ORDER, labelToken.getLine(),
				"In definition of task '"+taskname+"' the label '"+label
				+"' is used but was not defined." );
			
			return;
		}
		
		task.addLabel( label );	
	}
	
	protected void addDefTaskOutput( Token tasknameToken, DefTaskOutput output ) {
		
		String taskname;
		DefTask task;
		
		if( tasknameToken == null )
			throw new NullPointerException(
				"Task name token must not be null." );
		
		if( output == null )
			throw new NullPointerException(
				"Output variable must not be null." );
		
		taskname = tasknameToken.getText();
		
		task = defTaskMap.get( taskname );
		if( task == null )
			throw new NullPointerException( "Referenced task does not exits." );
		
		if( task.containsOutputWithName( output.getValue() ) ) {
			
			reportError( ERROR_UNIQUENESS, output.getLine(), "Duplicate use of output variable name." );
			
			return;
		}
		
		task.addOutput( output );
	}
	
	protected void addDefMacro( Token idToken ) {
		
		String name;
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		name = idToken.getText();
		
		addDefMacro( name );
	}
	
	protected void addDefMacro( String macroName ) {
		
		if( macroName == null )
			throw new NullPointerException( "Macro name must not be null." );
		
		if( macroName.isEmpty() )
			throw new RuntimeException( "Macro name must not be empty." );
		
		curDefMacro = new DefMacro( macroName );
		defMacroMap.put( macroName, curDefMacro );
	}
	
	protected void setDefMacroExpression( List<Expression> exprSet ) {
		
		if( exprSet == null )
			throw new NullPointerException(
				"Expression set must not be null." );
		
		if( exprSet.isEmpty() )
			throw new RuntimeException( "Expression set must not be empty." );
		
		curDefMacro.addExpression( exprSet );
	}
	
	protected void addDefMacroVar( Token idToken ) {
		
		String param;
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		param = idToken.getText();
		
		addDefMacroVar( param );
	}
	
	protected void addDefMacroVar( String paramName ) {
		
		if( paramName == null )
			throw new NullPointerException(
				"Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );
		
		curDefMacro.addVar( paramName );		
	}
	
	protected void addExtend() {
		curExtend = new Extend();
		extendList.add( curExtend );
	}
	
	protected void addExtend( List<Extend> list ) {
		
		if( list == null )
			throw new NullPointerException( "Extend list must not be null." );
		
		extendList.addAll( list );
	}
	
	protected void addExtendLabel( Token idToken ) {
		
		String label;
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		label = idToken.getText();
		
		if( label == null )
			throw new NullPointerException( "Label must not be null." );
		
		if( curExtend.containsLabel( label ) ) {
			
			reportError( ERROR_UNIQUENESS, idToken.getLine(),
				"Duplicate reference to label '"+label
				+"' in extend statement." );
			
			return;
		}
		
		curExtend.addLabel( label );
	}
	
	protected void addLabel( Token idToken ) {
		
		String label;
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		label = idToken.getText();

		// check whether we have seen a declare statement yet
		if( declare == null ) {
			
			reportError( ERROR_ORDER, idToken.getLine(), "Label definition for '"
				+label+"' must not appear prior to declare statement." );
			
			return;
		}
		
		// check whether this is the first time, this label is introduced
		if( labelMap.containsKey( label ) ) {
			
			reportError( ERROR_UNIQUENESS, idToken.getLine(),
				"Duplicate label definition. The label '"+label
				+"' has already been defined." );
			
			return;
		}
		
		labelMap.put( label, new HashSet<String>() );
	}
	
	protected void addLabel( Map<String,Set<String>> lm ) {
		
		if( lm == null )
			throw new NullPointerException( "Label map must not be null." );
		
		labelMap.putAll( lm );
	}
	
	protected void addLabelMember( Token outerToken, Token innerToken ) {
		
		String outer;
		String inner;
		Set<String> list;
		
		if( outerToken == null )
			throw new NullPointerException ("Outer token must not be null." );
		
		if( innerToken == null )
			throw new NullPointerException ("Inner token must not be null." );		
		
		outer = outerToken.getText();
		inner = innerToken.getText();

		// check whether the member refers to itself
		if( outer.equals( inner ) ) {
			
			reportError( ERROR_REFERENCE, innerToken.getLine(),
				"Label cannot have itself as a member." );
			
			return;
		}
		
		// check whether the inner label has been defined
		list = labelMap.get( inner );
		if( list == null ) {
			
			reportError( ERROR_ORDER, innerToken.getLine(),
				"Cannot reference undefined label '"+inner+"'." );
			
			return;
		}
		
		list = labelMap.get( outer );
		list.add( inner );
	}
	
	protected void addTarget() {
		
		if( !targetSet.isEmpty() )
			reportError(
				ERROR_UNIQUENESS, "Duplicate target variable definition statement." );
	}
	
	protected void addTarget( Token idToken ) {
		
		String target;
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		target = idToken.getText();
		
		if( target == null )
			throw new NullPointerException( "Target variable name must not be null." );
		
		if( target.isEmpty() )
			throw new NullPointerException( "Target variable name must not be empty." );
		
		if( targetSet.contains( target ) )
			reportError( ERROR_UNIQUENESS, idToken.getLine(),
				"Duplicate reference to variable '"+target
				+"' in target variable definition." );
		
		targetSet.add( target );
	}
	
	protected void checkPost() {
		
		if( declare == null )
			reportError( ERROR_EXISTENCE, "No declare statement found." );
	}
	
	protected void createMacroFromDefTask( Token taskNameToken ) {
		
		DefTask defTask;
		String taskName;
		List<Expression> exprList;
		ApplyExpression applyExpr;
		
		if( taskNameToken == null )
			throw new NullPointerException( "Task name token must not be null." );
		
		taskName = taskNameToken.getText();
		
		if( taskName == null )
			throw new NullPointerException( "Task name must not be null." );
		
		if( taskName.isEmpty() )
			throw new RuntimeException( "Task name must not be empty." );
		
		defTask = this.getDefTask( taskName );
		
		addDefMacro( taskName );
		
		exprList = new LinkedList<>();
		exprList.add( new IdExpression( taskName ) );
		
		applyExpr = new ApplyExpression();
		applyExpr.addParam( Constant.TOKEN_TASK, exprList );
		
		for( String paramName : defTask.getParamNameSet() ) {
			
			if( paramName.equals( Constant.TOKEN_TASK ) )
				continue;
			
			addDefMacroVar( paramName );
			
			exprList = new LinkedList<>();
			exprList.add( new IdExpression( paramName ) );
			applyExpr.addParam( paramName, exprList );
		}
		
		exprList = new LinkedList<>();
		exprList.add( applyExpr );
		
		setDefMacroExpression( exprList );		
	}
	
	protected List<Assign> getModifiableAssignList() {
		return assignList;
	}
	
	protected Set<String> getModifiableBlackList() {		
		return blackList;
	}
	
	protected Map<String,DefMacro> getModifiableDefMacroMap() {
		return defMacroMap;
	}
	
	protected Map<String,DefTask> getModifiableDefTaskMap() {
		return defTaskMap;
	}
	
	protected List<Extend> getModifiableExtendList() {
		return extendList;
	}
	
	protected Map<String,Set<String>> getModifiableLabelMap() {
		return labelMap;
	}
	
	protected void importFile( Token idToken ) {

		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		// check whether declare statement was present
		if( declare == null ) {
			
			reportError( ERROR_ORDER, idToken.getLine(),
				"Import is allowed only after declare statement." );
			
			return;
		}
		
		try {
			importFile( idToken.getText() );
		}
		catch( IOException e ) {
			reportError( ERROR_IO, idToken.getLine(), e.getMessage() );
		}

	}
	
	protected void importFile( String id ) throws IOException {
		
		ANTLRFileStream stream;
		CuneiformLexer lexer;
		CuneiformParser parser;
		String s;
		
		try {
			// create parser
			stream = new ANTLRFileStream( id );			
			lexer = new CuneiformLexer( stream );
			parser = new CuneiformParser( new CommonTokenStream( lexer ) );
			
			// copy everything that is relevant into the new parser
			parser.addLabel( labelMap );
			parser.addBlackListItem( blackList );
			parser.addDefTask( defTaskMap );
			parser.addAssign( assignList );
			parser.addDefMacro( defMacroMap );
			parser.addExtend( extendList );
			
			// run it
			parser.script();
		
			if( parser.hasError() ) {
				
				s = parser.getDeclare();
				if( s == null )
					s = id;
				
				reportError( ERROR_IMPORT, "Errors persist in '"+s+"'" );
				return;
			}
			
			// merge models
			labelMap = parser.getModifiableLabelMap();
			blackList = parser.getModifiableBlackList();
			defTaskMap = parser.getModifiableDefTaskMap();
			assignList = parser.getModifiableAssignList();
			defMacroMap = parser.getModifiableDefMacroMap();
			extendList = parser.getModifiableExtendList();
		}
		catch( DuplicateParseException e ) {
			// Suppressing duplicate parse
		}			

	}
	
	protected void setDeclare( Token idToken ) {
		
		String d;
		
		if( idToken == null )
			throw new NullPointerException( "Id token must not be null." );
		
		d = idToken.getText();
		
		if( d == null )
			throw new NullPointerException( "Declare text must not be null." );
		
		if( d.isEmpty() )
			throw new RuntimeException( "Declare text must not be empty." );
		
		if( declare != null ) {
			
			reportError( ERROR_UNIQUENESS, idToken.getLine(),
				"Duplicate declare statement. Already declared '"+declare
				+"'. Now trying to declare '"+d+"'." );
			
			return;
		}
		
		declare = d;
		
		if( blackList.contains( d ) )
			throw new DuplicateParseException();
		
		blackList.add( d );
	}
	
	protected void reportError( String type, int line, String msg ) {
		
		String s;
		
		if( type == null )
			throw new NullPointerException( "Error type must not be null." );
		
		if( type.isEmpty() )
			throw new RuntimeException( "Error type must not be empty." );
		
		if( line < 1 )
			throw new RuntimeException( "Line must be a positive number." );
		
		s = "";
		if( declare != null )
			s = " '"+declare+"'";
		
		reportError( type+" in"+s+" line "+line+": "+msg );
	}
	
	protected void reportError( String type, String msg ) {

		String s;
		
		if( type == null )
			throw new NullPointerException( "Error type must not be null." );
		
		if( type.isEmpty() )
			throw new RuntimeException( "Error type must not be empty." );
		
		s = "";
		if( declare != null )
			s = " in '"+declare+"'";
		
		reportError( type+s+": "+msg );		
	}
	
	protected void reportWarn( String type, int line, String msg ) {
		
		String s;
		
		if( type == null )
			throw new NullPointerException( "Warning type must not be null." );
		
		if( type.isEmpty() )
			throw new RuntimeException( "Warning type must not be empty." );
		
		if( line < 1 )
			throw new RuntimeException( "Line must be a positive number." );
		
		s = "";
		if( declare != null )
			s = " '"+declare+"'";
		
		reportWarn( type+" in"+s+" line "+line+": "+msg );
	}

	protected void reportWarn( String type, String msg ) {

		String s;
		
		if( type == null )
			throw new NullPointerException( "Warning type must not be null." );
		
		if( type.isEmpty() )
			throw new RuntimeException( "Warning type must not be empty." );
		
		s = "";
		if( declare != null )
			s = " in '"+declare+"'";
		
		reportWarn( type+s+": "+msg );		
	}

	protected void setDefTaskBody( Token tasknameToken, Token bodyToken ) {
		
		String taskname;
		String body;
		DefTask task;
		
		if( tasknameToken == null )
			throw new NullPointerException(
				"Task name token must not be null." );
		
		if( bodyToken == null )
			throw new NullPointerException(
				"Body token must not be null." );
		
		taskname = tasknameToken.getText();
		body = bodyToken.getText();
		
		task = defTaskMap.get( taskname );
		if( task == null )
			throw new NullPointerException( "Referenced task does not exits." );
		
		task.setBody( body );
	}
	
	protected void verifyDefTaskLabel( Token taskNameToken ) {
		
		String taskName;
		Set<String> labelSet;
		
		if( taskNameToken == null )
			throw new NullPointerException(
				"Task name token must not be null." );
		
		taskName = taskNameToken.getText();
		if( taskName == null )
			throw new NullPointerException( "Task name must not be null." );
		
		if( taskName.isEmpty() )
			throw new RuntimeException( "Task name must not be empty." );
		
		labelSet = getLabelSetForTask( taskName );
		
		if( !ForeignLangCatalog.hasLangLabel( labelSet ) ) {
			
			getDefTask( taskName ).addLabel( "bash" );
			labelSet = getLabelSetForTask( taskName );
		}
		
		if( !ForeignLangCatalog.isLangLabelUnique( labelSet ) )
			reportError( ERROR_UNIQUENESS, taskNameToken.getLine(),
			"Language label is not unique." );			
	}
}
