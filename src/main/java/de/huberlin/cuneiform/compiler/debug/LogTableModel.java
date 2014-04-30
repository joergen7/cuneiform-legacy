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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.swing.table.AbstractTableModel;

import de.huberlin.cuneiform.dag.JsonReportEntry;

public class LogTableModel extends AbstractTableModel {

	private static final long serialVersionUID = -1851606433233178678L;
	private static final String[] COL_NAME =
		{ "Timestamp", "Session UUID", "Task Sign.",
		  "Invoc. Sign.", "Task Name", "Msg. Type", "Msg Content" };
	
	private static final int COLID_TIMESTAMP = 0;
	private static final int COLID_SESSIONUUID = 1;
	private static final int COLID_TASKSIGNATURE = 2;
	private static final int COLID_INVOCSIGNATURE = 3;
	private static final int COLID_TASKNAME = 4;
	private static final int COLID_KEY = 5;
	private static final int COLID_PAYLOAD = 6;
	
	private List<String[]> data;
	
	public LogTableModel() {
		data = new LinkedList<>();
	}

	public void addReport( Set<JsonReportEntry> entrySet ) {
		
		if( entrySet == null )
			throw new NullPointerException( "Report entry set must not be null." );
		
		for( JsonReportEntry entry : entrySet )
			addReport( entry );
		
	}
	
	public void addReport( JsonReportEntry entry ) {
		
		int n;
		
		if( entry == null )
			throw new NullPointerException( "Report entry must not be null." );
		
		n = data.size();
		data.add( reportEntryToTuple( entry ) );
		fireTableRowsInserted( n, n );
	}
	
	@Override
	public int getColumnCount() {
		return COL_NAME.length;
	}
	
	@Override
	public String getColumnName( int col ) {
		return COL_NAME[ col ];
	}

	@Override
	public int getRowCount() {
		return data.size();
	}

	@Override
	public Object getValueAt( int row, int col ) {
		return data.get( row )[ col ];
	}

	private static String[] reportEntryToTuple( JsonReportEntry entry ) {
		
		String[] tuple;
		
		tuple = new String[ COL_NAME.length ];
		tuple[ COLID_TIMESTAMP ] = String.valueOf( entry.getTimestamp() );
		tuple[ COLID_SESSIONUUID ] = entry.getRunId().toString();
		tuple[ COLID_TASKSIGNATURE ] = String.valueOf( entry.getTaskId() );
		tuple[ COLID_INVOCSIGNATURE ] = String.valueOf( entry.getInvocId() );
		tuple[ COLID_TASKNAME ] = entry.getTaskName();
		tuple[ COLID_KEY ] = entry.getKey();
		tuple[ COLID_PAYLOAD ] = entry.getValue();
		
		return tuple;
		
	}
}
