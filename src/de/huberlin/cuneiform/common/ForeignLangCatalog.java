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

package de.huberlin.cuneiform.common;

import java.util.HashSet;
import java.util.Set;

public class ForeignLangCatalog {

	private static final String[] langArray = { "bash", "lisp", "octave", "r", "python", "perl", "scala" };
	
	public static final int LANGID_BASH = 0;
	public static final int LANGID_LISP = 1;
	public static final int LANGID_OCTAVE = 2;
	public static final int LANGID_R = 3;
	public static final int LANGID_PYTHON = 4;
	public static final int LANGID_PERL = 5;
	public static final int LANGID_SCALA = 6;
	
	public static String langIdToLabel( int id ) {
		return langArray[ id ];
	}
	
	public static int labelToLangId( String label ) {
		
		int i;
		
		if( label == null )
			throw new NullPointerException( "Label must not be null." );
		
		if( label.isEmpty() )
			throw new RuntimeException( "Label must not be empty." );
		
		for( i = 0; i < langArray.length; i++ )
			if( langArray[ i ].equals( label ) )
				return i;
		
		throw new RuntimeException(
			"A foreign language label with the name '"+label
			+"' does not exist." );
	}
	
	public static boolean isLangLabel( String label ) {
		
		int i;
		
		if( label == null )
			throw new NullPointerException( "Label must not be null." );
		
		if( label.isEmpty() )
			throw new RuntimeException( "Label must not be empty." );
		
		for( i = 0; i < langArray.length; i++ )
			if( langArray[ i ].equals( label ) )
				return true;
		
		return false;
	}
	
	public static boolean hasLangLabel( Set<String> labelSet ) {		
		return !getLangLabelSet( labelSet ).isEmpty();
	}
	
	public static boolean isLangLabelUnique( Set<String> labelSet ) {		
		return getLangLabelSet( labelSet ).size() == 1;
	}
	
	public static String getLangLabel( Set<String> labelSet ) {
		
		Set<String> langLabelSet;
		
		langLabelSet = getLangLabelSet( labelSet );
		
		if( langLabelSet.size() > 1 )
			throw new RuntimeException( "Language label in label set must be unique." );
		
		for( String langLabel : langLabelSet )
			return langLabel;
		
		throw new RuntimeException( "No language label in label set." );
	}
	
	public static int getLangLabelId( Set<String> labelSet ) {
		return labelToLangId( getLangLabel( labelSet ) );
	}
	
	public static Set<String> getLangLabelSet( Set<String> labelSet ) {
		
		Set<String> result;
		
		result = new HashSet<>();
		
		for( String label : labelSet )
			if( isLangLabel( label ) )
				result.add( label );
		
		return result;
	}
	
	public static String[] getLangLabelArray() {
		return langArray;
	}
}
