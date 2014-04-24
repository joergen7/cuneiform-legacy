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
import java.util.Set;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;

import de.huberlin.cuneiform.dag.JsonReportEntry;

public class ContentOverview extends JPanel {

	private static final long serialVersionUID = -799053449381971678L;
	
	private SrcContentTab srcContentTab;
	private LogContentTab logContentTab;

	public ContentOverview( JFrame frame ) {
		
		JTabbedPane tabbedPane;
		
		setLayout( new BorderLayout() );

		logContentTab = new LogContentTab();
		srcContentTab = new SrcContentTab( frame );
		
		tabbedPane = new JTabbedPane();
		add( tabbedPane, BorderLayout.CENTER );
		tabbedPane.addTab( "Log", logContentTab );
		tabbedPane.addTab( "Output", new OutputContentTab() );
		tabbedPane.addTab( "Task def.", new DefTaskContentTab() );
		tabbedPane.addTab( "Statistics", new StatContentTab() );
		tabbedPane.addTab( "Task graph", new TaskGraphContentTab() );
		tabbedPane.addTab( "Invoc. Graph", new InvocGraphContentTab() );
		tabbedPane.addTab( "Source Code", srcContentTab );
	}
	
	public void addInputFile( String filename ) {
		srcContentTab.addInputFile( filename );
	}
	
	public void addReport( Set<JsonReportEntry> report ) {
		logContentTab.addReport( report );
	}
}
