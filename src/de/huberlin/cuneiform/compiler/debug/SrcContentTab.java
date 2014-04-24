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
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

public class SrcContentTab extends JPanel implements ItemListener {

	private static final long serialVersionUID = -3567545950192124478L;
	private JComboBox<String> uriBox;
	private JTextArea srcArea;
	private JFrame frame;
	private Map<String,File> fileMap;
	
	public SrcContentTab( JFrame frame ) {
		
		JPanel panel;
		
		setFrame( frame );
		fileMap = new HashMap<>();
		
		setLayout( new BorderLayout() );
		
		panel = new JPanel();
		panel.setLayout( new BorderLayout() );
		add( panel, BorderLayout.NORTH );
		
		panel.add( new JLabel( "Source file:" ), BorderLayout.WEST );
		
		uriBox = new JComboBox<>();
		uriBox.addItemListener( this );
		panel.add( uriBox, BorderLayout.CENTER );
		
		srcArea = new JTextArea();
		srcArea.setEditable( false );
		add( new JScrollPane( srcArea ), BorderLayout.CENTER );
	}

	public void addInputFile( String filename ) {
		
		File f;
		
		f = new File( filename );
		fileMap.put( f.getName(), f );
		uriBox.addItem( f.getName() );
	}

	@Override
	public void itemStateChanged( ItemEvent e ) {
		
		StringBuffer buf;
		String line;
		
		buf = new StringBuffer();
		
		try(
			BufferedReader reader =
				new BufferedReader(
					new FileReader( fileMap.get( uriBox.getSelectedItem() ) ) ) ) {
			
			while( ( line = reader.readLine() ) != null )
				buf.append( line ).append( '\n' );
			
			srcArea.setText( buf.toString() );
			
			
		}
		catch( FileNotFoundException e1 ) {
			
			JOptionPane.showMessageDialog(
				frame,
			    e1.getMessage(),
			    "FileNotFoundException",
			    JOptionPane.ERROR_MESSAGE );
			
			e1.printStackTrace();
		}
		catch( IOException e1 ) {
			
			e1.printStackTrace();

			JOptionPane.showMessageDialog(
					frame,
				    e1.getMessage(),
				    "IOException",
				    JOptionPane.ERROR_MESSAGE );
			
		}
		
		
		
	}
	
	public void setFrame( JFrame frame ) {
		
		if( frame == null )
			throw new NullPointerException( "Frame must not be null." );
		
		this.frame = frame;
	}
}
