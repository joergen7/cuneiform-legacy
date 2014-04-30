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

package de.huberlin.cuneiform.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.json.JSONException;

import de.huberlin.cuneiform.compiler.debug.DebugDispatcher;
import de.huberlin.cuneiform.compiler.local.LocalDispatcher;
import de.huberlin.cuneiform.dag.CuneiformDag;
import de.huberlin.cuneiform.dag.NotDerivableException;

public class Main {
	
	private static final int PLATFORM_DOT = 0;
	private static final int PLATFORM_LOCAL = 1;
	private static final int PLATFORM_DEBUG = 2;
	private static final String LABEL_VERSION = "version 1.0 build 2014-04-30";

	public static void main( String[] args )
	throws ParseException, IOException, NotDerivableException, InterruptedException, JSONException {
		
		GnuParser gnuParser;
		CommandLine cmdline;
		Options opt;
		String value;
		int platform;
		File outputDir;
		String[] fileList;
		StringBuffer buf;
		String line;
		String dagid;
		File logFile;
		
		opt = new Options();
		
		opt.addOption( "p", "platform", true,
			"The platform to perform the Cuneiform script's interpretation. "
			+"Possible platforms are: 'dot', 'local', and 'debug'. Default is 'local'." );
		
		opt.addOption( "d", "directory", true,
			"The output directory, to put the interpretation intermediate and output result as well as the default location to store the log." );
		
		opt.addOption( "c", "clean", false,
			"If set, the execution engine ignores all cached results and starts a clean workflow run." );
		
		opt.addOption( "r", "runid", true, "If set, a custom id is set for this workflow run. By default a UUID string is used." );
		
		opt.addOption( "f", "file", true, "Override the default location of the log file and use the specified filename instead. If the platform is 'dot', this option sets the name of the output dot-file." );
		
		opt.addOption( "h", "help", false, "Print help text." );
		
		gnuParser = new GnuParser();
		cmdline = gnuParser.parse( opt, args );
		
		if( cmdline.hasOption( "help" ) ) {
			
			System.out.println(
				"CUNEIFORM - A Functional Workflow Language\n"+LABEL_VERSION );
			new HelpFormatter().printHelp(
				"java -jar cuneiform.jar [OPTION]*", opt );
			
			return;
		}
		
		if( cmdline.hasOption( "platform" ) ) {
			
			value = cmdline.getOptionValue( "platform" );
			
			if( value.equals( "dot" ) )
				platform = PLATFORM_DOT;
			else if( value.equals( "local" ) )
				platform = PLATFORM_LOCAL;
			else if( value.equals( "debug" ) )
				platform = PLATFORM_DEBUG;
			else
				throw new RuntimeException(
					"Specified platform '"+value+"' not recognized." );
				
		}
		else
			platform = PLATFORM_LOCAL;
		
		if( cmdline.hasOption( 'd' ) ) {
			
			value = cmdline.getOptionValue( 'd' );
		}
		else
			value = "build";
		
		outputDir = new File( value );
		
		
		
		if( outputDir.exists() ) {
			
			if( !outputDir.isDirectory() )
				throw new IOException(
					"Output directory '"+outputDir.getAbsolutePath()
					+"' exists but is not a directory." );
			
			else
				if( cmdline.hasOption( 'c' ) ) {
					
					FileUtils.deleteDirectory( outputDir );
					
					if( !outputDir.mkdirs() )
						throw new IOException(
							"Could not create output directory '"
							+outputDir.getAbsolutePath()+"'" );
				}
		}
		else
			if( !outputDir.mkdirs() )
				throw new IOException(
					"Could not create output directory '"
					+outputDir.getAbsolutePath()+"'" );
		
		if( cmdline.hasOption( 'r' ) )
			dagid = cmdline.getOptionValue( 'r' );
		else
			dagid = UUID.randomUUID().toString();
		
		if( cmdline.hasOption( 'f' ) )
			logFile = new File( cmdline.getOptionValue( 'f' ) );
		else
			logFile = null;
			
		
		fileList = cmdline.getArgs();
		buf = new StringBuffer();
		if( fileList.length == 0 ) {
			
			try( BufferedReader reader = new BufferedReader( new InputStreamReader( System.in ) ) ) {
				
				while( ( line = reader.readLine() ) != null )
					buf.append( line ).append( '\n' );
			}
			
			switch( platform ) {
			
				case PLATFORM_DOT   : createDot( buf.toString(), outputDir, logFile ); break;
				case PLATFORM_LOCAL : runLocal( buf.toString(), outputDir, logFile, dagid ); break;
				case PLATFORM_DEBUG : runDebug( buf.toString(), outputDir, logFile, dagid ); break;
				default             : throw new RuntimeException( "Platform not recognized." );
			}
		}
		else

			switch( platform ) {
			
				case PLATFORM_DOT   : createDot( fileList, outputDir, logFile ); break;
				case PLATFORM_LOCAL : runLocal( fileList, outputDir, logFile, dagid ); break;
				case PLATFORM_DEBUG : runDebug( fileList, outputDir, logFile, dagid ); break;
				default             : throw new RuntimeException( "Platform not recognized." );
			}

	}
	
	private static void createDot( String[] inputFileList, File outputDir, File dotFile ) throws IOException {
		
		CuneiformDag dag;
		File df;
				
		dag = new CuneiformDag();
		for( String inputFile : inputFileList )
			dag.addInputFile( inputFile );

		if( dotFile == null )
			df = new File( outputDir.getAbsolutePath()+"/graph_task_"+dag.getDagId()+".dot" );
		else
			df = dotFile;
		
		try( BufferedWriter writer = new BufferedWriter( new FileWriter( df, false ) ) ) {
			
			writer.write( dag.toDot() );
		}
	}
	
	private static void createDot( String inputString, File outputDir, File dotFile  ) throws IOException {
		
		CuneiformDag dag;
		File df;

		dag = new CuneiformDag();
		dag.addInputString( inputString );
		
		if( dotFile == null )
			df = new File( outputDir.getAbsolutePath()+"/graph_task_"+dag.getDagId()+".dot" );
		else
			df = dotFile;
		
		try( BufferedWriter writer = new BufferedWriter( new FileWriter( df, false ) ) ) {
			
			writer.write( dag.toDot() );
		}
	}
	
	private static void runLocal( String[] inputFileList, File outputDir, File logFile, String dagid )
	throws IOException, NotDerivableException, InterruptedException, JSONException {
		
		LocalDispatcher dispatcher;
		
		dispatcher = new LocalDispatcher( outputDir, logFile, dagid );
		for( String inputFile : inputFileList )
			dispatcher.addInputFile( inputFile );
		dispatcher.run();
		
	}
	
	private static void runLocal( String inputString, File outputDir, File logFile, String dagid )
	throws IOException, NotDerivableException, InterruptedException, JSONException {
		
		LocalDispatcher dispatcher;
		
		dispatcher = new LocalDispatcher( outputDir, logFile, dagid );
		dispatcher.addInputString( inputString );
		dispatcher.run();
		
	}
	
	
	private static void runDebug( String[] inputFileList, File outputDir, File logFile, String dagid )
	throws IOException, NotDerivableException {
		
		DebugDispatcher dispatcher;
		
		dispatcher = new DebugDispatcher( outputDir, logFile, dagid );
		for( String inputFile : inputFileList )
			dispatcher.addInputFile( inputFile );
		dispatcher.run();
		
	}
	
	private static void runDebug( String inputString, File outputDir, File logFile, String dagid )
	throws NotDerivableException {
		
		DebugDispatcher dispatcher;
		
		dispatcher = new DebugDispatcher( outputDir, logFile, dagid );
		dispatcher.addInputString( inputString );
		dispatcher.run();
		
	}

}
