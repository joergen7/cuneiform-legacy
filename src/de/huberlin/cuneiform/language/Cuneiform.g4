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

grammar Cuneiform ;

options { superClass = BaseCuneiformParser; }

@lexer::header {
package de.huberlin.cuneiform.language;
}

@parser::header {
package de.huberlin.cuneiform.language;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
}

// PARSER RULES

// top level definition

script     : toplevel* EOF             { checkPost(); } ;

toplevel   : importFile | declare | label | deftask | extend | assign | target
           | defmacro ;

// expressions

importFile : ( IMPORT|INCLUDE )id=ID   { importFile( $id ); }
             SEMICOLON ;

declare    : DECLARE id=ID SEMICOLON   { setDeclare( $id ); } ;

label      : LABEL outer=ID            { addLabel( $outer ); }
             ( COLON( inner=ID         { addLabelMember( $outer, $inner ); }
             )+ )? SEMICOLON ;

deftask    : DEFTASK n=ID              { addDefTask( $n ); }
             ( IN( i=ID                { addDefTaskLabel( $n, $i ); }
             )+ )?                     { verifyDefTaskLabel( $n ); }
             LPAREN( vout=outvar       { addDefTaskOutput( $n, $vout.v ); }
             )* COLON( pin=param       { addDefTaskParam( $n, $pin.p ); }
             )* RPAREN b=BODY          { setDefTaskBody( $n, $b );
                                         createMacroFromDefTask( $n ); } ;

param returns [DefTaskParam p]
           : s=singleParam             { $p = $s.p; }
           | r=reduceParam             { $p = $r.p; }
           | c=correlParam             { $p = $c.p; } ;

singleParam returns [CorrelParam p]
           :                           { int type = 0; }
             ( TILDE                   { type = 1; }
             )? i=ID                   { $p = new CorrelParam( $i, type ); } ;

reduceParam returns [ReduceParam p]
           :                           { int type = 0; }
             ( TILDE                   { type = 1; }
             )? LTAG i=ID RTAG         { $p = new ReduceParam( $i, type ); } ;

correlParam returns [CorrelParam p]
           : LSQUAREBR                 { $p = new CorrelParam(); int type; }
             (                         { type = 0; }
             ( TILDE                   { type = 1; }
             )? k=ID                   { $p.addParam( $k, type ); }
             )+ RSQUAREBR ;
             
outvar returns [DefTaskOutput v]
           :                           { int type = 0; }
             ( QUOTE                   { type = 2; }
             | TILDE                   { type = 1; }
             )?( LTAG i=ID RTAG        { $v = new ReduceOutput( $i, type ); }
             | j=ID                    { $v = new SingleOutput( $j, type ); }
             ) ;

extend     : EXTEND                    { addExtend(); }
             ( id=ID                   { addExtendLabel( $id ); }
             )+( b=before              { addAuxMethod( $b.am ); }
             | a=after                 { addAuxMethod( $a.am ); }
             | fi=forinput             { addAuxMethod( $fi.am ); }
             | fo=foroutput            { addAuxMethod( $fo.am ); }
             )+ ;

before returns [BeforeMethod am]
           : f=BEFORE                  { $am = new BeforeMethod( $f.getLine() ); }
             ( LPAREN COLON      
             ( id=ID                   { $am.addVar( $id ); }
             )* RPAREN )? b=BODY       { $am.setBody( $b ); } ;

after returns [AfterMethod am]
           : f=AFTER                   { $am = new AfterMethod( $f.getLine() ); }
             ( LPAREN              
             ( id=ID                   { $am.addVar( $id ); }
             )* COLON RPAREN )? b=BODY { $am.setBody( $b ); } ;

forinput returns [ForInputMethod am]
           : FOR INPUT id=ID           { $am = new ForInputMethod( $id ); }
             b=BODY                    { $am.setBody( $b ); } ;

foroutput returns [ForOutputMethod am]
           : FOR OUTPUT id=ID          { $am = new ForOutputMethod( $id ); }
             b=BODY                    { $am.setBody( $b ); } ;

assign     :                           { addAssign(); }
             ( i=ID                    { addAssignVar( $i ); }
             )+ EQ e=expression        { addAssignExpression( $e.r ); }
             SEMICOLON ;

expression returns [List<Expression> r]
           :                           { $r = new LinkedList<>(); }
             ( m=macroexpr             { $r.add( $m.r ); }
             | id=ID                   { $r.add( new IdExpression( $id ) ); }
             | a=applyexpr             { $r.add( $a.r ); }
             | s=stringexpr            { $r.add( $s.r ); }
             )+ ;
             
macroexpr returns [MacroExpression r]
           : i=ID LPAREN               { $r = new MacroExpression( $i ); }
             ( p=ID COLON e=expression { $r.addParam( $p, $e.r ); }
             )* RPAREN ;

applyexpr returns [ApplyExpression r]
           : APPLY LPAREN              { $r = new ApplyExpression(); }
             ( p=ID COLON e=expression { $r.addParam( $p, $e.r ); }
             )* RPAREN ;

stringexpr returns [StringExpression r]
           :                           { boolean stage = true; }
             ( TILDE                   { stage = false; }
             )?( s=STRING1             { $r = new StringExpression( $s, stage ); }
             | s=STRING2               { $r = new StringExpression( $s, stage ); } ) ;

target     : TARGET                    { addTarget(); }
             ( id=ID                   { addTarget( $id ); }
             )+ SEMICOLON ;

defmacro   : DEFMACRO id=ID LPAREN     { addDefMacro( $id ); }
             ( v=ID                    { addDefMacroVar( $v ); }
             )* RPAREN e=expression    { setDefMacroExpression( $e.r ); }
             SEMICOLON ;
               
// LEXER RULES

// comments

COMMENT1   : ( '#'|'//'|'%' )~'\n'*    { skip(); };
COMMENT2   : '/*' .*? '*/'             { skip(); };

// keywords

AFTER      : 'after' ;
APPLY      : 'apply' ;
BEFORE     : 'before' ;
COLON      : ':' ;
DECLARE    : 'declare' ;
DEFMACRO   : 'defmacro' ;
DEFTASK    : 'deftask' ;
EQ         : '=' ;
EXTEND     : 'extend' ;
FOR        : 'for' ;
IMPORT     : 'import' ;
IN         : 'in' ;
INCLUDE    : 'include' ;
INPUT      : 'input' ;
LABEL      : 'label' ;
LPAREN     : '(' ;
LSQUAREBR  : '[' ;
LTAG       : '<' ;
OUTPUT     : 'output' ;
PREFIX     : 'prefix' ;
QUOTE      : '`' ;
TARGET     : 'target' ;
RPAREN     : ')' ;
RSQUAREBR  : ']' ;
RTAG       : '>' ;
SEMICOLON  : ';' ;
TILDE      : '~' ;

// non-keywords

BODY       : '*{' .*? '}*' ;
STRING1    : '\'' .*? '\'' ;
STRING2    : '"' .*? '"' ;
ID         : [a-zA-Z0-9/\.\-_\+\*]+ ;
WS         : [ ,\n\t\r]                { skip(); } ;
ILLEGAL    : . ;
