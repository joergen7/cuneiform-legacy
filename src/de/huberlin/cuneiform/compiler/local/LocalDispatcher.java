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

package de.huberlin.cuneiform.compiler.local;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import de.huberlin.cuneiform.dag.ExecDag;
import de.huberlin.cuneiform.dag.Invocation;
import de.huberlin.cuneiform.dag.JsonReportEntry;
import de.huberlin.cuneiform.dag.NotDerivableException;

public class LocalDispatcher extends ExecDag {
	
	private File buildDir;
	private File logFile;
	
	public static final String SCRIPT_FILENAME = "__script__";
	public static final String SUCCESS_FILENAME = "__success__";
	// private static final int NTHREADS = 4;
	
	public LocalDispatcher( File buildDir ) {
		setBuildDir( buildDir );
	}

	public LocalDispatcher( File buildDir, File logFile ) {
		setBuildDir( buildDir );		
		setLogFile( logFile );
	}

	public LocalDispatcher( File buildDir, File logFile, String dagid ) {
		super( dagid );
		setBuildDir( buildDir );
		setLogFile( logFile );
	}
	
	public File getLogFile() {
		
		if( logFile == null )
			return new File( buildDir.getAbsolutePath()+"/log_"+getDagId()+".txt" );
		
		return logFile;
	}
	
	public void run()
	throws IOException, InterruptedException, NotDerivableException {
		
		Set<Invocation> invocationSet;
		if( buildDir == null )
			throw new NullPointerException( "Build directory must not be null." );
		
		if( !buildDir.exists() )
			if( !buildDir.mkdirs() )
				throw new IOException(
					"Unable to create sandbox folder '"
					+buildDir.getAbsolutePath()+"'." );
		
		do {
			
			invocationSet = getReadyInvocationSet();
			
			for( Invocation invoc : invocationSet )				
				evalReport( dispatch( invoc ) );
			
		} while( !invocationSet.isEmpty() );		
	}
	
	public void setBuildDir( File buildDir ) {
		
		if( buildDir == null )
			throw new NullPointerException( "Build directory must not be null." );
		
		this.buildDir = buildDir;
	}
	
	public void setLogFile( File logFile ) {
		this.logFile = logFile;
	}
	
	protected Set<JsonReportEntry> dispatch( Invocation invocation )
	throws IOException, InterruptedException, NotDerivableException {
		
		File scriptFile;
		Process process;
		int exitValue;
		Set<JsonReportEntry> report;
		String line;
		String[] arg;
		String value;
		int i;
		StringBuffer buf;
		File location;
		File reportFile;
		StreamConsumer stdoutConsumer, errConsumer;
		ExecutorService executor;
		String signature;
		Path srcPath, destPath;
		File successMarker;
		
		if( invocation == null )
			throw new NullPointerException( "Invocation must not be null." );
		
		if( !invocation.isReady() )
			throw new RuntimeException( "Cannot dispatch invocation that is not ready." );
		
		location = new File( buildDir.getAbsolutePath()+"/"+invocation.getSignature() );
		successMarker = new File( location.getAbsolutePath()+"/"+SUCCESS_FILENAME );
		reportFile = new File( location.getAbsolutePath()+"/"+Invocation.REPORT_FILENAME );
		
		if( !successMarker.exists() ) {
			
			if( location.exists() )
				FileUtils.deleteDirectory( location );
		
			if( !location.mkdirs() )
				throw new IOException( "Could not create invocation location." );
			
			scriptFile = new File( location.getAbsolutePath()+"/"+SCRIPT_FILENAME );
			
			try( BufferedWriter writer = new BufferedWriter( new FileWriter( scriptFile, false ) ) ) {
				
				// write away script
				writer.write( invocation.toScript() );
				
				
			}
			
			scriptFile.setExecutable( true );
			
			for( String filename : invocation.getStageInList() ) {
				
				

				if( filename.charAt( 0 ) != '/' && filename.indexOf( '_' ) >= 0 ) {

					signature = filename.substring( 0, filename.indexOf( '_' ) );
					
					srcPath = FileSystems.getDefault().getPath( buildDir.getAbsolutePath()+"/"+signature+"/"+filename );				
					destPath = FileSystems.getDefault().getPath( buildDir.getAbsolutePath()+"/"+invocation.getSignature()+"/"+filename );
					Files.createSymbolicLink( destPath, srcPath );
				}
			}
	
			
			arg = new String[] {
					"/usr/bin/time",
					"-a",
					"-o",
					location.getAbsolutePath()+"/"+Invocation.REPORT_FILENAME,
					"-f",
					"{"
					+JsonReportEntry.ATT_TIMESTAMP+":"+System.currentTimeMillis()+","
					+JsonReportEntry.ATT_RUNID+":\""+invocation.getDagId()+"\","
					+JsonReportEntry.ATT_TASKID+":"+invocation.getTaskNodeId()+","
					+JsonReportEntry.ATT_TASKNAME+":\""+invocation.getTaskName()+"\","
					+JsonReportEntry.ATT_LANG+":\""+invocation.getLangLabel()+"\","
					+JsonReportEntry.ATT_INVOCID+":"+invocation.getSignature()+","
					+JsonReportEntry.ATT_KEY+":\""+JsonReportEntry.KEY_INVOC_TIME+"\","
					+JsonReportEntry.ATT_VALUE+":"
					+"{\"realTime\":%e,\"userTime\":%U,\"sysTime\":%S,"
					+"\"maxResidentSetSize\":%M,\"avgResidentSetSize\":%t,"
					+"\"avgDataSize\":%D,\"avgStackSize\":%p,\"avgTextSize\":%X,"
					+"\"nMajPageFault\":%F,\"nMinPageFault\":%R,"
					+"\"nSwapOutMainMem\":%W,\"nForcedContextSwitch\":%c,"
					+"\"nWaitContextSwitch\":%w,\"nIoRead\":%I,\"nIoWrite\":%O,"
					+"\"nSocketRead\":%r,\"nSocketWrite\":%s,\"nSignal\":%k}}",
					scriptFile.getAbsolutePath() };
			
			
			
			// run script
			process = Runtime.getRuntime().exec( arg, null, location );
			
			executor = Executors.newCachedThreadPool();
			
			stdoutConsumer = new StreamConsumer( process.getInputStream() );
			executor.execute( stdoutConsumer );
			
			errConsumer = new StreamConsumer( process.getErrorStream() );
			executor.execute( errConsumer );
			
			executor.shutdown();
			
			exitValue = process.waitFor();
			if( !executor.awaitTermination( 4, TimeUnit.SECONDS ) )
				throw new RuntimeException(
					"Consumer threads did not finish orderly." );
			
			
			try( BufferedWriter reportWriter = new BufferedWriter( new FileWriter( reportFile, true ) ) ) {
	
			
				if( exitValue != 0 ) {
					
					System.err.println( "[script]" );
					
					try( BufferedReader reader = new BufferedReader( new StringReader( invocation.toScript() ) ) ) {
						
						i = 0;
						while( ( line = reader.readLine() ) != null )
							System.err.println( String.format( "%02d  %s", ++i, line ) );
					}
					
					System.err.println( "[out]" );
					try( BufferedReader reader = new BufferedReader( new StringReader( stdoutConsumer.getContent() ) ) ) {
						
						while( ( line = reader.readLine() ) != null )
							System.err.println( line );
					}
					
					System.err.println( "[err]" );
					try( BufferedReader reader = new BufferedReader( new StringReader( errConsumer.getContent() ) ) ) {
						
						while( ( line = reader.readLine() ) != null )
							System.err.println( line );
					}
					
					System.err.println( "[end]" );
					
					throw new RuntimeException(
						"Invocation of task '"+invocation.getTaskName()
						+"' with signature "+invocation.getSignature()
						+" terminated with non-zero exit value. Exit value was "
						+exitValue+"." );
				}
				
				
				try( BufferedReader reader = new BufferedReader( new StringReader( stdoutConsumer.getContent() ) ) ) {
					
					buf = new StringBuffer();
					while( ( line = reader.readLine() ) != null )
						buf.append( line.replaceAll( "\\\\", "\\\\\\\\" ).replaceAll( "\"", "\\\"" ) ).append( '\n' );
					
					
					value = buf.toString();
					if( !value.isEmpty() )
					
					reportWriter.write( new JsonReportEntry( invocation, JsonReportEntry.KEY_INVOC_STDOUT, value ).toString() );
				}
				try( BufferedReader reader = new BufferedReader( new StringReader( errConsumer.getContent() ) ) ) {
					
					buf = new StringBuffer();
					while( ( line = reader.readLine() ) != null )
						buf.append( line.replaceAll( "\\\\", "\\\\\\\\" ).replaceAll( "\"", "\\\"" ) ).append( '\n' );
					
					value = buf.toString();
					if( !value.isEmpty() )
					
					reportWriter.write( new JsonReportEntry( invocation, JsonReportEntry.KEY_INVOC_STDERR, value ).toString() );
				}
		
			}			
		}
		
		// gather report
		report = new HashSet<>();
		try(
			BufferedReader reader =
				new BufferedReader( new FileReader( reportFile ) ) ) {
			
			while( ( line = reader.readLine() ) != null ) {
				
				line = line.trim();
				
				if( line.isEmpty() )
					continue;
				
				report.add( new JsonReportEntry( line ) );
			}
			
		}
		
		invocation.evalReport( report );

		if( !successMarker.exists() )
			if( !successMarker.createNewFile() )
				throw new IOException( "Could not create success marker." );
		
		return report; 
	}
	
	protected void evalReport( Set<JsonReportEntry> report ) throws IOException {
		
		try( BufferedWriter writer = new BufferedWriter( new FileWriter( getLogFile(), true ) ) ) {
			
			for( JsonReportEntry entry : report ) {
				writer.write( entry.toString() );
				writer.write( '\n' );
				System.out.println( entry );
			}

		}
	}
	

}
