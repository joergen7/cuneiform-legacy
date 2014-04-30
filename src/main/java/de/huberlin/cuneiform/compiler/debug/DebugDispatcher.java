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
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JSplitPane;
import javax.swing.WindowConstants;

import org.json.JSONException;

import de.huberlin.cuneiform.compiler.local.LocalDispatcher;
import de.huberlin.cuneiform.dag.Invocation;
import de.huberlin.cuneiform.dag.NotDerivableException;
import de.huberlin.cuneiform.dag.JsonReportEntry;

public class DebugDispatcher extends LocalDispatcher implements ActionListener {
	
	private InvocOverview invocOverview;
	private ContentOverview contentOverview;
	private JMenuItem exitItem;
	private JFrame frame;
	private JMenuItem openItem;

	public DebugDispatcher( File buildDir, File logFile, String dagid ) {
		
		super( buildDir, logFile, dagid );
		
		JSplitPane splitPane;
		
		frame = new JFrame( "Cuneiform Debug Interface" );
		frame.setSize( 700, 500 );
		frame.setDefaultCloseOperation( WindowConstants.DISPOSE_ON_CLOSE );
		frame.setLayout( new BorderLayout() );
		
		addMenu();
		
		invocOverview = new InvocOverview( this );
		contentOverview = new ContentOverview( frame );
		
		splitPane = new JSplitPane( JSplitPane.HORIZONTAL_SPLIT,
            invocOverview, contentOverview );
		// splitPane.setResizeWeight( .5 );
		splitPane.setDividerLocation( 350 );
		
		frame.add( splitPane, BorderLayout.CENTER );
		
	}
	
	@Override
	public void actionPerformed( ActionEvent e ) {
		
		List<Integer> signatureList;
		Set<Invocation> readyInvocationSet;
		Set<JsonReportEntry> report;
		
		
		if( e.getSource() == exitItem ) {
			
			frame.dispose();
			return;
		}
		
		if( e.getActionCommand().equals( PreInvocView.LABEL_STEP ) ) {
						
			signatureList = invocOverview.getSelectedPreInvocSignatureList();
			readyInvocationSet = getReadyInvocationSet();
			
			if( signatureList == null )
				throw new NullPointerException( "Signature list must not be null." );

			if( signatureList.isEmpty() )
				throw new RuntimeException( "Signature list must not be empty." );

			invocOverview.setContentOld();

			try {
				
				for( Integer i : signatureList ) {
					
					if( i == null )
						throw new NullPointerException( "Signature must not be null." );
					
					for( Invocation invoc : readyInvocationSet )
						
						if( invoc.getSignature() == i ) {
							
							report = dispatch( invoc );
							
							if( !invoc.isComputed() )
								throw new RuntimeException(
									"Expected the invocation to be computed." );
							
							evalReport( report );
							
							readyInvocationSet.remove( invoc );
	
							invocOverview.removePreInvocation( invoc );
							invocOverview.insertPostInvocation( invoc );
							
							
							break;
						}
					
					updateView();
				}
			}
			catch( NotDerivableException e1 ) {
				
				e1.printStackTrace();
				
				JOptionPane.showMessageDialog(
						frame,
					    e1.getMessage(),
					    "NotDerivableException",
					    JOptionPane.ERROR_MESSAGE );

			}
			catch( IOException e1 ) {

				e1.printStackTrace();
				
				JOptionPane.showMessageDialog(
						frame,
					    e1.getMessage(),
					    "IOException",
					    JOptionPane.ERROR_MESSAGE );

			}
			catch( InterruptedException e1 ) {

				e1.printStackTrace();
				
				JOptionPane.showMessageDialog(
						frame,
					    e1.getMessage(),
					    "InterruptedException",
					    JOptionPane.ERROR_MESSAGE );

			} catch ( JSONException e1 ) {
				
				e1.printStackTrace();
				
				JOptionPane.showMessageDialog(
						frame,
					    e1.getMessage(),
					    "JSONException",
					    JOptionPane.ERROR_MESSAGE );
			}
			
			
			return;
		}
	}
	
	@Override
	public void addInputFile( String filename ) throws IOException {
		
		contentOverview.addInputFile( filename );
		super.addInputFile( filename );
	}
	
	@Override
	public void run() throws NotDerivableException {
		
		
		updateView();
		frame.setVisible( true );
		
	}
	
	private void addMenu() {
		
		JMenuBar menuBar;
		JMenu fileMenu;
		JMenu helpMenu;
		JMenuItem aboutItem;
		
		menuBar = new JMenuBar();
		frame.setJMenuBar( menuBar );
		
		fileMenu = new JMenu( "File" );
		menuBar.add( fileMenu );
		
		openItem = new JMenuItem( "Open File ..." );
		fileMenu.add( openItem );
		
		exitItem = new JMenuItem( "Exit" );
		exitItem.addActionListener( this );
		fileMenu.add( exitItem );
		
		
		helpMenu = new JMenu( "Help" );
		menuBar.add( helpMenu );
		
		aboutItem = new JMenuItem( "About Cuneiform" );
		helpMenu.add( aboutItem );
	}

	private void updateView() throws NotDerivableException {
		
		Set<Invocation> preSet;
		
		preSet = getNonComputedInvocationSet();
		invocOverview.insertPreInvocation( preSet );
	}

	@Override
	protected void evalReport( Set<JsonReportEntry> report ) {
		contentOverview.addReport( report );
	}

}
