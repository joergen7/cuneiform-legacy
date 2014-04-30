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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.antlr.v4.runtime.Token;

import de.huberlin.cuneiform.common.Constant;
import de.huberlin.cuneiform.common.ForeignLangCatalog;
import de.huberlin.cuneiform.dag.ParamItem;
import de.huberlin.cuneiform.dag.Typeable;

public class DefTask extends RawBodyContainer {

	private Set<String> labelSet;
	private String taskName;
	private Set<DefTaskParam> paramSet;
	private List<DefTaskOutput> outputList;
	
	public DefTask( Token idToken ) {
		this( idToken.getText() );
	}
	
	public DefTask( String name ) {
		
		setTaskName( name );
		
		labelSet = new HashSet<>();
		paramSet = new HashSet<>();
		outputList = new LinkedList<>();
	}
	
	public void addParam( DefTaskParam p ) {
		
		ParamItem taskItem;
		
		if( p == null )
			throw new NullPointerException(
				"Input variable set must not be null." );
		
		if( p.isEmpty() )
			throw new RuntimeException(
				"Input variable set must not be empty." );
		
		if( p.containsItemWithValue( Constant.TOKEN_TASK ) ) {
			
			if( isTaskParamExplicit() )
				throw new RuntimeException(
					"Special task parameter cannot be referenced more than "
					+"once." );
			
			taskItem = p.getItemByValue( Constant.TOKEN_TASK );
			taskItem.setType( Typeable.TYPE_STAGELESS );			
		}
		
		if( p instanceof ReduceParam && p.containsItemWithValue( Constant.TOKEN_TASK ) )
			throw new RuntimeException(
				"Special task parameter cannot be marked reduce." );
		
		paramSet.add( p );
	}
	
	public void addLabel( Token idToken ) {
		
		if( idToken == null )
			throw new NullPointerException( "Label token must not be null." );
		
		addLabel( idToken.getText() );
	}
	
	public void addLabel( String label ) {
		
		if( label == null )
			throw new NullPointerException( "Label must not be null." );
		
		if( label.isEmpty() )
			throw new RuntimeException( "Label must not be empty." );
		
		labelSet.add( label );
	}
	
	public void addOutput( DefTaskOutput outvar ) {
		
		if( outvar == null )
			throw new NullPointerException(
				"Output variable name must not be null." );
		
		if( outputList.contains( outvar ) )
			throw new RuntimeException(
				"Duplicate definition of output variable name '"+outvar+"'." );
		
		outputList.add( outvar );
	}
	
	public boolean containsExplicitParam( ParamItem invar ) {
		
		if( invar == null )
			throw new NullPointerException( "Parameter item must not be null." );
		
		for( DefTaskParam param : getParamSet() )
			if( param.contains( invar ) )
				return true;
		
		return false;
	}
	
	public boolean containsExplicitParamWithName( String invar ) {
		
		if( invar == null )
			throw new NullPointerException(
				"Input variable name must not be null." );
		
		if( invar.isEmpty() )
			throw new RuntimeException(
				"Input variable name must not be empty." );
		
		for( DefTaskParam ingrp : getParamSet() )
			if( ingrp.containsItemWithValue( invar ) )
				return true;
		
		return false;
	}
	
	public boolean containsLabel( String label ) {
		
		if( label == null )
			throw new NullPointerException( "Label must not be null." );
		
		if( label.isEmpty() )
			throw new RuntimeException( "Label must not be empty." );

		return labelSet.contains( label );
	}
	
	public boolean containsOutputWithName( String outputName ) {
		
		if( outputName == null )
			throw new NullPointerException(
				"Output variable name must not be null." );
		
		if( outputName.isEmpty() )
			throw new RuntimeException(
				"Output variable name must not be empty." );

		for( DefTaskOutput output : outputList )
			if( output.getValue().equals( outputName ) )
				return true;
		
		return false;
	}
	
	public Set<String> getLabelSet() {
		return Collections.unmodifiableSet( labelSet );
	}
	
	public String getLangLabel() {
		return ForeignLangCatalog.getLangLabel( getLabelSet() );
	}
	
	public int getLangLabelId() {
		return ForeignLangCatalog.getLangLabelId( getLabelSet() );
	}
	
	public Set<DefTaskParam> getNonTaskParamSet() {
		
		Set<DefTaskParam> result;
		
		result = new HashSet<>();
		result.addAll( getParamSet() );
		result.remove( getTaskParam() );
		
		return result;
	}
	
	public int getOutputType( int outputChannel ) {
		return outputList.get( outputChannel ).getType();
	}
	
	public int getOutputType( String outputName ) {
		
		for( DefTaskOutput output : outputList )
			if( output.getValue().equals( outputName ) )
				return output.getType();
		
		throw new RuntimeException(
			"An output with the name '"+outputName+"' does not exist." );
			
	}
	
	public Set<String> getParamNameSet() {
		
		Set<String> nameSet;
		
		nameSet = new HashSet<>();
		
		for( DefTaskParam param : getParamSet() )
			for( ParamItem s : param )
				nameSet.add( s.getValue() );
		
		return nameSet;
	}
	
	public Set<DefTaskParam> getParamSet() {
		
		Set<DefTaskParam> set;
		CorrelParam taskParam;
		
		set = new HashSet<>();
		set.addAll( paramSet );
		
		if( !isTaskParamExplicit() ) {
			
			taskParam = new CorrelParam();
			taskParam.add( new ParamItem( Constant.TOKEN_TASK, Typeable.TYPE_DEFTASK ) );
			
			set.add( taskParam );
		}

		
		return set;
	}
	
	public Set<ReduceOutput> getReduceOutputSet() {
		
		Set<ReduceOutput> set;
		
		set = new HashSet<>();
		
		for( DefTaskOutput output : outputList )
			if( output instanceof ReduceOutput )
				set.add( ( ReduceOutput )output );
		
		return set;
	}
	
	public Set<String> getReduceOutputNameSet() {
		
		Set<String> set;
		
		set = new HashSet<>();
		
		for( ReduceOutput output : getReduceOutputSet() )
			set.add( output.getValue() );
		
		return set;
	}
	
	public Set<SingleOutput> getSingleOutputSet() {
		
		Set<SingleOutput> set;
		
		set = new HashSet<>();
		
		for( DefTaskOutput output : outputList )
			if( output instanceof SingleOutput )
				set.add( ( SingleOutput )output );
		
		return set;
	}
	
	public Set<String> getSingleOutputNameSet() {
		
		Set<String> set;
		
		set = new HashSet<>();
		
		for( SingleOutput output : getSingleOutputSet() )
			set.add( output.getValue() );
		
		return set;
	}
	
	public int getOutputChannel( String outputName ) {
		
		int i;
		
		for( i = 0; i < outputList.size(); i++ )
			if( outputList.get( i ).getValue().equals( outputName ) )
				return i;
		
		throw new RuntimeException( "An output channel with the name '"+outputName+"' does not exist." );
	}
	
	public List<DefTaskOutput> getOutputList() {
		return Collections.unmodifiableList( outputList );
	}
	
	public String getOutputName( int outputChannel ) {
		return outputList.get( outputChannel ).getValue();
	}
	
	public List<String> getOutputNameList() {
		
		List<String> nameList;
		
		nameList = new LinkedList<>();
		
		for( DefTaskOutput output : outputList )
			nameList.add( output.getValue() );
		
		return nameList;
	}
	
	public String getTaskName() {
		return taskName;
	}
	
	public CorrelParam getTaskParam() {
		
		for( DefTaskParam param : getParamSet() )
			if( param.containsItemWithValue( Constant.TOKEN_TASK ) ) {
				
				if( !( param instanceof CorrelParam ) )
					throw new RuntimeException(
						"Task parameter cluster must be instance of "
						+"CorrelParam." );
				
				return ( CorrelParam )param;
			}
		
		throw new RuntimeException(
			"The task parameter is not part of the parameter definition, which "
			+"is bad." );
	}
	
	public boolean isOutputReduce( String outputName ) {
		
		if( outputName == null )
			throw new NullPointerException( "Output name must not be null." );
		
		if( outputName.isEmpty() )
			throw new RuntimeException( "Output name must not be empty." );
		
		for( DefTaskOutput output : outputList )
			if( output.getValue().equals( outputName ) )
				return output instanceof ReduceOutput;
		
		throw new RuntimeException(
			"Output '"+outputName+"' not contained in deftask parameter set." );
	}
	
	public boolean isOutputReduce( int outputChannel ) {
		return outputList.get( outputChannel ) instanceof ReduceOutput;
	}
	
	public boolean isOutputStage( String outputName ) {
		
		if( outputName == null )
			throw new NullPointerException( "Output name must not be null." );
		
		if( outputName.isEmpty() )
			throw new RuntimeException( "Output name must not be empty." );

		for( DefTaskOutput output : outputList )
			if( output.getValue().equals( outputName ) )
				return output.isStage();
		
		throw new RuntimeException(
			"Output '"+outputName+"' not contained in deftask parameter set." );
	}
	
	public boolean isOutputStage( int outputChannel ) {
		return outputList.get( outputChannel ).isStage();
	}
	
	public boolean isParamReduce( String paramName ) {
		
		if( paramName == null )
			throw new NullPointerException( "Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );

		if( paramName.equals( Constant.TOKEN_TASK ) )
			return false;
		
		for( DefTaskParam param : paramSet )
			if( param.containsItemWithValue( paramName ) )
				return param instanceof ReduceParam;
		
		throw new RuntimeException(
			"Parameter '"+paramName
			+"' not contained in deftask parameter set." );		
	}

	public boolean isParamStage( String paramName ) {
		
		if( paramName == null )
			throw new NullPointerException( "Parameter name must not be null." );
		
		if( paramName.isEmpty() )
			throw new RuntimeException( "Parameter name must not be empty." );

		if( paramName.equals( Constant.TOKEN_TASK ) )
			return false;
		
		for( DefTaskParam param : paramSet )
			if( param.containsItemWithValue( paramName ) )
				return param.getItemByValue( paramName ).isStage();
		
		throw new RuntimeException(
			"Parameter '"+paramName
			+"' not contained in deftask parameter set." );
	}
	
	public boolean isTaskParamExplicit() {
		
		for( DefTaskParam param : paramSet )
			if( param.containsItemWithValue( Constant.TOKEN_TASK ) )
				return true;
		
		return false;		
	}	

	public int nOutputChannel() {
		return outputList.size();
	}
	
	public int outputIndexOf( String outputName ) {
		
		int i;
		
		if( outputName == null )
			throw new NullPointerException( "Output name must not be null." );
		
		if( outputName.isEmpty() )
			throw new RuntimeException( "Output name must not be empty." );
		
		
		for( i = 0; i < outputList.size(); i++ )
			if( outputList.get( i ).getValue().equals( outputName ) )
				return i;
		
		throw new RuntimeException( "Output with name '"+outputName+"' not found." );

	}
	
	public int outputTypeOf( int outputIndex ) {
		return outputList.get( outputIndex ).getType();
	}
	
	public int outputTypeOf( String outputName ) {
		return outputTypeOf( outputIndexOf( outputName ) );
	}
	
	public void setTaskName( String taskName ) {
		
		if( taskName == null )
			throw new NullPointerException( "Name must not be null." );
		
		if( taskName.isEmpty() )
			throw new RuntimeException( "Name must not be empty." );

		this.taskName = taskName;
	}
	
	@Override
	public String toString() {
		
		
		String ret;
		
		ret = "deftask "+taskName;
		
		if( !labelSet.isEmpty() ) {
			
			ret += " in";
			for( String label : labelSet )
				ret += " "+label;
		}
		
		ret += "(";
		for( DefTaskOutput outvar : outputList )
			ret += " "+outvar;
		
		ret += " :";
		for( DefTaskParam invarSet : paramSet )

			if( invarSet.size() == 1 )
				if( invarSet instanceof ReduceParam ) {
					ret += " <";
					for( ParamItem invar : invarSet )
						ret += invar.getValue();
					ret += ">";
				}
				else
					for( ParamItem invar : invarSet )
						ret += " "+invar.getValue(); // TODO: type is ignored.
			else {
				ret += " [";
				for( ParamItem invar : invarSet )
					ret += " "+invar.getValue();
				ret += " ]";
			}
		
		ret += " ) *{"+getBody()+"}*";
		
		return ret;
		
	}
	
}
