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

package de.huberlin.cuneiform.compiler.debug;

import java.awt.BorderLayout;
import java.util.List;
import java.util.Set;

import javax.swing.JPanel;
import javax.swing.JSplitPane;

import de.huberlin.cuneiform.dag.Invocation;
import de.huberlin.cuneiform.dag.NotDerivableException;

public class InvocOverview extends JPanel {

	private static final long serialVersionUID = -2539284697260268182L;
	
	private PreInvocView preInvocView;
	private PostInvocView postInvocView;
	
	public InvocOverview( DebugDispatcher dispatcher ) {
		
		JSplitPane splitPane;
		
		setLayout( new BorderLayout() );
		
		preInvocView = new PreInvocView( dispatcher );
		postInvocView = new PostInvocView();
		
		splitPane = new JSplitPane(
			JSplitPane.VERTICAL_SPLIT,
			preInvocView,
			postInvocView );
		add( splitPane );
		
		splitPane.setResizeWeight( .5 );
	}
	
	public List<Integer> getSelectedPreInvocSignatureList() {
		return preInvocView.getSelectedInvocSignatureList();
	}
	
	public void insertPostInvocation( Invocation invoc ) throws NotDerivableException {
		
		if( invoc == null )
			throw new NullPointerException( "Invocation must not be null." );
		
		postInvocView.insertInvocation( invoc );
	}
	
	public void insertPreInvocation( Invocation invoc ) throws NotDerivableException {
		
		if( invoc == null )
			throw new NullPointerException( "Invocation must not be null." );
		
		preInvocView.insertInvocation( invoc );
	}
	
	public void insertPreInvocation( Set<Invocation> invocSet ) throws NotDerivableException {
		
		if( invocSet == null )
			throw new NullPointerException( "Invocation set must not be null." );
		
		preInvocView.insertInvocation( invocSet );
		
	}
	
	public void removePreInvocation( Invocation invoc ) {
		preInvocView.removeInvocation( invoc );
	}
	
	public void setContentOld() {
		preInvocView.setContentOld();
		postInvocView.setContentOld();
	}
}
