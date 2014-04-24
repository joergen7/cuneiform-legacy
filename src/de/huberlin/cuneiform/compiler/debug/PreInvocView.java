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

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

public class PreInvocView extends PostInvocView implements ListSelectionListener {

	public static final String LABEL_STEP = "Step";
	private static final long serialVersionUID = 6764937943759533934L;
	
	
	private JButton stepButton;
	
	public PreInvocView( DebugDispatcher dispatcher ) {
		
		JPanel panel;
		JButton stepReadyButton;
		JButton runButton;
			
		setBorder( BorderFactory.createTitledBorder( "Pre-execution" ) );
		getSelectionModel().addListSelectionListener( this );

		
		panel = new JPanel();
		add( panel, BorderLayout.SOUTH );
		
		panel.setLayout( new BoxLayout( panel, BoxLayout.X_AXIS ) );
		
		stepButton = new JButton( LABEL_STEP );
		stepButton.setEnabled( false );
		stepButton.addActionListener( dispatcher );
		
		panel.add( stepButton );
		
		stepReadyButton = new JButton( "Step ready" );
		panel.add( stepReadyButton );
		
		runButton = new JButton( "Run" );
		panel.add( runButton );
	}
		
	@Override
	public void valueChanged( ListSelectionEvent e ) {
		
		List<Integer> signatureList;
		
		signatureList = getSelectedInvocSignatureList();
		
		if( signatureList.isEmpty() )
			stepButton.setEnabled( false );
		else
			stepButton.setEnabled( true );
		
	}

}
