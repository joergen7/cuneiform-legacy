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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.huberlin.cuneiform.dag.Invocation;
import de.huberlin.cuneiform.dag.JsonReportEntry;
import de.huberlin.cuneiform.dag.NotDerivableException;

public class InvocationCaller implements Callable<Set<JsonReportEntry>> {
	
	private Invocation invocation;
	private File sandbox;
	
	public InvocationCaller( Invocation invocation, File sandbox ) {
		setInvocation( invocation );
		setSandbox( sandbox );
	}
	
	public void setInvocation( Invocation invocation ) {
		
		if( invocation == null )
			throw new NullPointerException( "Invocation must not be null." );
		
		if( !invocation.isReady() )
			throw new RuntimeException( "Cannot dispatch invocation that is not ready." );

		this.invocation = invocation;
	}
	
	public void setSandbox( File sandbox ) {
		
		if( sandbox == null )
			throw new NullPointerException( "Sandbox must not be null." );
		
		this.sandbox = sandbox;
	}

	@Override
	public Set<JsonReportEntry> call() throws NotDerivableException, IOException, InterruptedException {
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
		
		location = new File( sandbox.getAbsolutePath()+"/"+invocation.getSignature() );
		reportFile = new File( location.getAbsolutePath()+"/"+Invocation.REPORT_FILENAME );
		
		if( !location.exists() ) {
		
			if( !location.mkdirs() )
				throw new IOException( "Could not create invocation location." );
			
			scriptFile = new File( location.getAbsolutePath()+"/"+LocalDispatcher.SCRIPT_FILENAME );
			
			try( BufferedWriter writer = new BufferedWriter( new FileWriter( scriptFile, false ) ) ) {
				
				// write away script
				writer.write( invocation.toScript() );
				
				
			}
			
			scriptFile.setExecutable( true );
			
			for( String filename : invocation.getStageInList() ) {
				
				signature = filename.substring( 0, filename.indexOf( '_' ) );
				srcPath = FileSystems.getDefault().getPath( sandbox.getAbsolutePath()+"/"+signature+"/"+filename );
				destPath = FileSystems.getDefault().getPath( sandbox.getAbsolutePath()+"/"+invocation.getSignature()+"/"+filename );
				Files.createSymbolicLink( destPath, srcPath );
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
				+JsonReportEntry.ATT_INVOCID+":"+invocation.getSignature()+","
				+JsonReportEntry.ATT_TASKNAME+":\""+invocation.getTaskName()+"\","
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

		
		
		return report; 

	}

}
