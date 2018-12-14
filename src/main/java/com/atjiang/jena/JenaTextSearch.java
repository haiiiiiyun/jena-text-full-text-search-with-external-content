/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.atjiang.jena;

import org.apache.jena.atlas.lib.StrUtils ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.query.* ;
import org.apache.jena.query.text.EntityDefinition ;
import org.apache.jena.query.text.TextDatasetFactory ;
import org.apache.jena.query.text.TextIndexConfig;
import org.apache.jena.rdf.model.Model ;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.sparql.util.QueryExecUtils ;
import org.apache.jena.vocabulary.RDFS ;
import org.apache.lucene.store.Directory ;
import org.apache.lucene.store.RAMDirectory ;
import org.apache.lucene.store.SimpleFSDirectory;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Build a text search dataset */
public class JenaTextSearch
{
    static { LogCtl.setLog4j() ; }
    static Logger log = LoggerFactory.getLogger("JenaTextSearch") ;
    
    public static void main(String ... argv)
    {
        Dataset ds = createCode() ;
        queryData(ds) ;
    }
    
    public static Dataset createCode() 
    {
        // Base data
        Dataset ds1 = DatasetFactory.create() ;
        Model defaultModel = ModelFactory.createDefaultModel();
        defaultModel.read("data.ttl", "N-TRIPLES");
        ds1.setDefaultModel(defaultModel);

        // Define the index mapping
        EntityDefinition entDef = new EntityDefinition( "uri", "text", ResourceFactory.createProperty( App.EMAIL_URI_PREFIX, "content" ) );

        Directory dir = null;
        try {
            dir = new SimpleFSDirectory(Paths.get("index")); // lucene index directory
        }
        catch( IOException e){
            e.printStackTrace();
        }

        // Join together into a dataset
        Dataset ds = TextDatasetFactory.createLucene( ds1, dir, new TextIndexConfig(entDef) ) ;
        
        return ds ;
    }

    public static void queryData(Dataset dataset)
    {
        String prefix = "PREFIX email: <" + App.EMAIL_URI_PREFIX + "> " +
                "PREFIX text: <http://jena.apache.org/text#> ";

        long startTime = System.nanoTime() ;
        System.out.println("Email's content contains 'good'");
        String query = "SELECT * WHERE " +
                "{ ?s text:query (email:content 'good') ." +
                "  ?s email:content ?text . " +
                " }";

        dataset.begin(ReadWrite.READ) ;
        try {
            Query q = QueryFactory.create(prefix+"\n"+query) ;
            QueryExecution qexec = QueryExecutionFactory.create(q , dataset) ;
            QueryExecUtils.executeQuery(q, qexec) ;
        } finally { dataset.end() ; }
        long finishTime = System.nanoTime() ;
        double time = (finishTime-startTime)/1.0e6 ;
        System.out.println("Query " + String.format("FINISH - %.2fms", time)) ;

        startTime = System.nanoTime() ;
        System.out.println("Email's content contains 'bad'");
        query = "SELECT * WHERE " +
                "{ (?s ?score ?lit) text:query (email:content 'bad' \"highlight:s:<em class='hiLite'> | e:</em>\") ." +
                "  ?s email:content ?text . " +
                " }";

        dataset.begin(ReadWrite.READ) ;
        try {
            Query q = QueryFactory.create(prefix+"\n"+query) ;
            QueryExecution qexec = QueryExecutionFactory.create(q , dataset) ;
            QueryExecUtils.executeQuery(q, qexec) ;
        } finally { dataset.end() ; }
        finishTime = System.nanoTime() ;
        time = (finishTime-startTime)/1.0e6 ;
        System.out.println("Query " + String.format("FINISH - %.2fms", time)) ;
    }

}

