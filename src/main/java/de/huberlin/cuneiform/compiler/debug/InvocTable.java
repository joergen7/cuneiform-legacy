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

import javax.swing.JTable;

import de.huberlin.cuneiform.dag.Invocation;
import de.huberlin.cuneiform.dag.NotDerivableException;

public class InvocTable extends JTable {

	private static final long serialVersionUID = -4437552399853689181L;
	
	public InvocTable() {
		super( new InvocTableModel() );
		this.setDefaultRenderer( Object.class, new InvocTableCellRenderer() );
	}
	
	/** Returns the signatures of all selected invocations.
	 * 
	 * @return The signature list.
	 */
	public List<Integer> getSelectedInvocSignatureList() {
		
		List<Integer> signatureList;
		int[] idxArray;
		InvocTableModel model;
		Integer signature;
		
		model = getInvocTableModel();
		
		signatureList = new LinkedList<>();
		
		idxArray = getSelectedRows();
		for( int idx : idxArray ) {
			
			signature = model.getSignatureInRow( idx );
			
			if( signature == null )
				continue;
			
			signatureList.add( signature );
		}
		
		return signatureList;
	}
	public void insertInvocation( Invocation invoc ) throws NotDerivableException {
		getInvocTableModel().insertInvocation( invoc );
	}

	public void insertInvocation( Set<Invocation> invocSet ) throws NotDerivableException {
		getInvocTableModel().insertInvocation( invocSet );		
	}
	
	public boolean isReadyByRow( int row ) {
		return getInvocTableModel().isRowReady( row );
	}
	
	public boolean isRowNew( int row ) {
		return getInvocTableModel().isRowNew( row );
	}
	
	public void removeInvocation( Invocation invoc ) {
		getInvocTableModel().removeInvocation( invoc );
	}
	
	public void setContentOld() {
		getInvocTableModel().setContentOld();
	}
	
	public InvocTableModel getInvocTableModel() {
		return ( InvocTableModel )getModel();
	}
	
}
