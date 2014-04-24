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

import java.awt.Color;
import java.awt.Component;

import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

public class InvocTableCellRenderer extends DefaultTableCellRenderer {

	private static final Color COLOR_READY_NEW = new Color( 30, 100, 0 );
	private static final Color COLOR_ENUMERABLE_NEW = new Color( 120, 160, 120 );
	private static final Color COLOR_READY_OLD = new Color( 40, 40, 40 );
	private static final Color COLOR_ENUMERABLE_OLD = new Color( 130, 130, 130 );
	
	private static final long serialVersionUID = -1475643337503207661L;
	
	@Override
	public Component getTableCellRendererComponent( JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column ) {
		
		Component component;
		InvocTable invocTable;
		
		component = super.getTableCellRendererComponent( table, value, isSelected, hasFocus, row, column );
		
		invocTable = ( InvocTable )table;
		
		if( invocTable.isReadyByRow( row ) )
			
			if( invocTable.isRowNew( row ) )
				super.setForeground( COLOR_READY_NEW );
			else
				super.setForeground( COLOR_READY_OLD );
		
		else
			
			if( invocTable.isRowNew( row ) )
				super.setForeground( COLOR_ENUMERABLE_NEW );
			else
				super.setForeground( COLOR_ENUMERABLE_OLD );
		
		return component;
	}

}
