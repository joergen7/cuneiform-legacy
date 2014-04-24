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

package de.huberlin.cuneiform.dag;

import de.huberlin.cuneiform.common.ForeignLangCatalog;

public class PerlInvocation extends Invocation {

	protected PerlInvocation( TaskNode taskNode ) throws NotDerivableException {
		super( taskNode );
	}

	@Override
	public String callFunction(String name, String... argValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String defFunction(String funName, String outputName,
			String[] inputNameList, String body) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String defFunctionLog() throws NotDerivableException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCheckPost() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getShebang() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String varDef(String varname, DataList list)
			throws NotDerivableException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String varDef(String varname, String value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String newList(String listName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String listAppend(String listName, String element) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String dereference(String varName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String forEach(String listName, String elementName, String body) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String ifNotFileExists(String fileName, String body) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String raise(String msg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String join(String... elementList) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String quote(String content) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String fileSize(String filename) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String ifListIsNotEmpty(String listName, String body) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String listToBraceCommaSeparatedString(String listName,
			String stringName, String open, String close) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String defFunctionNormalize() throws NotDerivableException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String symlink(String src, String dest) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getImport() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String comment( String comment ) {
		return "# "+comment.replace( "\n", "\n# " )+"\n";
	}

	@Override
	public String copyArray(String from, String to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getLangId() {
		return ForeignLangCatalog.LANGID_PERL;
	}
}
