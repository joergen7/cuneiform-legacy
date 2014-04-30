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
import java.util.Set;

import javax.swing.table.AbstractTableModel;

import de.huberlin.cuneiform.dag.Invocation;
import de.huberlin.cuneiform.dag.NotDerivableException;

public class InvocTableModel extends AbstractTableModel {

	private static final long serialVersionUID = -3610028326945533777L;
	private static final String[] COL_NAME = { "Signature", "Task", "Language", "State" };
	private static final String TOKEN_NEW = "new";
	private static final String TOKEN_OLD = "old";
	
	private static final int NCOL_FULL = 7;
	
	private static final int COLID_SIGNATURE = 0;
	private static final int COLID_TASK = 1;
	private static final int COLID_LANGUAGE = 2;
	private static final int COLID_STATE = 3;
	private static final int COLID_ID = 4;
	private static final int COLID_STATEID = 5;
	private static final int COLID_GENERATION = 6;
	
	private LinkedList<String[]> data;
	
	public InvocTableModel() {
		data = new LinkedList<>();
	}

	public boolean containsInvocationWithSignature( int signature ) {
		return getInvocationRowById( signature ) >= 0;
	}
	
	@Override
	public int findColumn( String columnName ) {
		
		int i;
		
		for( i = 0; i < COL_NAME.length; i++ )
			if( COL_NAME[ i ].equals( columnName ) )
				return i;
		
		return -1;
	}
	
	@Override
	public int getColumnCount() {
		return COL_NAME.length;
	}

	@Override
	public String getColumnName( int col ) {
		return COL_NAME[ col ];
	}
	
	public int getInvocationRow( Invocation invoc ) {
		return getInvocationRowById( invoc.getId() );
	}
	
	public int getInvocationRowById( int id ) {
		
		int i;
		
		for( i = 0; i < data.size(); i++ )
			if( data.get( i )[ COLID_ID ].equals( String.valueOf( id ) ) )
				return i;
		
		return -1;
	}
	
	@Override
	public int getRowCount() {
		return data.size();
	}
	
	public Integer getSignatureInRow( int idx ) {
		
		int signature;
		
		try {
			signature = Integer.valueOf( data.get( idx )[ COLID_SIGNATURE ] );
		}
		catch( NumberFormatException e ) {
			return null;
		}
		
		return signature;
	}

	@Override
	public Object getValueAt( int row, int col ) {
		return data.get( row )[ col ];
	}
	
	public void insertInvocation( Invocation invoc ) throws NotDerivableException {
		
		int i;
		
		if( invoc == null )
			throw new NullPointerException( "Invocation must not be null." );
		
		i = getInvocationRow( invoc );
		
		if( i < 0 )
			pushInvocation( invoc );
		else {
			setInvocationInRow( i, invoc );
		}
	}
	
	public void insertInvocation( Set<Invocation> invocSet ) throws NotDerivableException {
		
		if( invocSet == null )
			throw new NullPointerException( "Invocation set must not be null." );
		
		for( Invocation invoc : invocSet )
			insertInvocation( invoc );
	}
	
	public boolean isRowNew( int row ) {
		return data.get( row )[ COLID_GENERATION ].equals( TOKEN_NEW );
	}
	
	public boolean isRowReady( int row ) {
		return data.get( row )[ COLID_STATEID ].equals( String.valueOf( Invocation.STATEID_READY ) );
	}
	
	public void pushInvocation( Invocation invoc ) throws NotDerivableException {
		
		String[] tuple;
		
		tuple = invocationToTuple( invoc );
		tuple[ COLID_GENERATION ] = TOKEN_NEW;
		
		data.push( tuple );
		fireTableDataChanged();
	}
	
	public void removeInvocation( Invocation invoc ) {
		removeInvocationById( invoc.getId() );
	}
	
	public void removeInvocationById( int id ) {
		
		int i, n;
		String idString;
		
		n = data.size();
		idString = String.valueOf( id );
		
		for( i = 0; i < n; i++ )
			if( data.get( i )[ COLID_ID ].equals( idString ) ) {
				
				data.remove( i );
				fireTableDataChanged();
				return;
			}
		
		throw new RuntimeException( "An invocation with the id "+id+" is not registered." );
	}
	
	public void setInvocationInRow( int idx, Invocation invoc ) throws NotDerivableException {
		
		String[] tuple;
		String[] tuple0;
		
		tuple0 = data.get( idx );
		tuple = invocationToTuple( invoc );
		
		if( !tuple0[ COLID_STATEID ].equals( tuple[ COLID_STATEID ] ) )
			tuple[ COLID_GENERATION ] = TOKEN_NEW;
		
		data.set( idx, tuple );
		fireTableRowsUpdated( idx, idx );
	}
	
	public void setContentOld() {
		
		for( String[] tuple : data )
			tuple[ COLID_GENERATION ] = TOKEN_OLD;
		
		fireTableDataChanged();
	}

	@Override
	public void setValueAt( Object aValue, int rowIndex, int columnIndex ) {
		
		String[] tuple;
		
		tuple = data.get( rowIndex );
		tuple[ columnIndex ] = aValue.toString();
		fireTableCellUpdated( rowIndex, columnIndex );
	}
	
	private static String[] invocationToTuple( Invocation invoc ) throws NotDerivableException {
		
		String[] tuple;
		
		tuple = new String[ NCOL_FULL ];

		try {
			tuple[ COLID_SIGNATURE ] = String.valueOf( invoc.getSignature() );
		}
		catch( NotDerivableException e ) {
			tuple[ COLID_SIGNATURE ] = "[nil]";
		}
		tuple[ COLID_TASK ] = invoc.getTaskName();
		tuple[ COLID_LANGUAGE ] = invoc.getLangLabel();
		tuple[ COLID_STATE ] = invoc.getStateLabel();
		tuple[ COLID_ID ] = String.valueOf( invoc.getId() );
		tuple[ COLID_STATEID ] = String.valueOf( invoc.getStateId() );
		tuple[ COLID_GENERATION ] = TOKEN_OLD;
		
		return tuple;
	}
}
