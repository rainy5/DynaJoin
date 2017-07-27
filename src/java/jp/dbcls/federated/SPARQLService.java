/*
 * Copyright (C) , DBCLS.ROIS.JP
 *
*/
package jp.dbcls.federated;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.QueryManager;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.util.EndpointFactory;
import com.fluidops.fedx.util.QueryStringUtil;
import com.fluidops.fedx.util.Version;
import java.io.File;
import java.io.IOException;
//import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
//import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.NullAppender;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;

public class SPARQLService
{
  protected String fedxConfig = null;
  protected int verboseLevel = 0;
  protected boolean logtofile = false;
  protected String prefixDeclarations = null;
  protected List<Endpoint> endpoints = new ArrayList();
  protected OutputFormat outputFormat = OutputFormat.STDOUT;
  protected List<String> queries = new ArrayList();
  protected Repository repo = null;
  protected String outFolder = null;

  public static void main(String[] args)
  {
    try
    {
      new SPARQLService().run(args);
    } catch (Exception e) {
      System.out.println("Error while using the FedX CLI. System will exit. \nDetails: " + e.getMessage());
      System.exit(1);
    }
  }

  public void run(String[] args)
  {
    configureRootLogger();

    System.out.println("FedX Cli " + Version.getLongVersion());

    parse(args);

    configureLogging();

    if (Config.getConfig().getDataConfig() != null)
    {
      if (this.endpoints.size() > 0)
        System.out.println("WARN: Mixture of implicitely and explicitely specified federation members, dataConfig used: " + Config.getConfig().getDataConfig());
      try {
        List additionalEndpoints = EndpointFactory.loadFederationMembers(new File(Config.getConfig().getDataConfig()));
        this.endpoints.addAll(additionalEndpoints);
      } catch (FedXException e) {
        error("Failed to load implicitly specified data sources from fedx configuration. Data config is: " + Config.getConfig().getDataConfig() + ". Details: " + e.getMessage(), false);
      }

    }

    if (this.endpoints.size() == 0) {
      error("No federation members specified. At least one data source is required.", true);
    } else {
      System.out.println("The endpoint list:");
      for (int i = 0; i < this.endpoints.size(); i++) {
        System.out.println(this.endpoints.get(i));
      }

    }

    try
    {
      this.repo = FedXFactory.initializeFederation(this.endpoints);
    }
    catch (FedXException e)
    {
      error("Problem occured while setting up the federation: " + e.getMessage(), false);
    }

    if (Config.getConfig().getPrefixDeclarations() == null)
      initDefaultPrefixDeclarations();
  }

  protected void parse(String[] _args)
  {
    if (_args.length == 0) {
      printUsage(new boolean[] { true });
    }

    if ((_args.length == 1) && (_args[0].equals("-help"))) {
      printUsage(new boolean[] { true });
    }

    List args = new LinkedList(Arrays.asList(_args));

    parseConfiguaration(args, false);
    try
    {
      Config.initialize(new String[] { this.fedxConfig });
      if (this.prefixDeclarations != null)
        Config.getConfig().set("prefixDeclarations", this.prefixDeclarations);
    }
    catch (FedXException e) {
      error("Problem occured while setting up the federation: " + e.getMessage(), false);
    }

    parseEndpoints(args, false);
    parseOutput(args);

    if (this.outFolder == null)
      this.outFolder = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(new Date());
  }

  protected void parseConfiguaration(List<String> args, boolean printError)
  {
    String arg = (String)args.get(0);

    if (arg.equals("-c")) {
      readArg(args, new String[0]);
      this.fedxConfig = readArg(args, new String[] { "path/to/fedxConfig.ttl" });
    }
    else if (arg.equals("-verbose")) {
      this.verboseLevel = 1;
      readArg(args, new String[0]);
      try {
        this.verboseLevel = Integer.parseInt((String)args.get(0));
        readArg(args, new String[0]);
      }
      catch (Exception numberFormat)
      {
      }

    }
    else if (arg.equals("-logtofile")) {
      readArg(args, new String[0]);
      this.logtofile = true;
    }
    else if (arg.equals("-p")) {
      readArg(args, new String[0]);
      this.prefixDeclarations = readArg(args, new String[] { "path/to/prefixDeclarations.prop" });
    }
    else if (printError) {
      error("Unxpected Configuration Option: " + arg, false);
    } else {
      return;
    }

    parseConfiguaration(args, false);
  }

  protected void parseEndpoints(List<String> args, boolean printError)
  {
    String arg = (String)args.get(0);

    if (arg.equals("-s")) {
      readArg(args, new String[0]);
      String url = readArg(args, new String[] { "urlToSparqlEndpoint" });
      try {
        Endpoint endpoint = EndpointFactory.loadSPARQLEndpoint(url);
        this.endpoints.add(endpoint);
      } catch (FedXException e) {
        error("SPARQL endpoint " + url + " could not be loaded: " + e.getMessage(), false);
      }

    }
    else if (arg.equals("-l")) {
      readArg(args, new String[0]);
      String path = readArg(args, new String[] { "path/to/NativeStore" });
      try {
        Endpoint endpoint = EndpointFactory.loadNativeEndpoint(path);
        this.endpoints.add(endpoint);
      } catch (FedXException e) {
        error("NativeStore " + path + " could not be loaded: " + e.getMessage(), false);
      }

    }
    else if (arg.equals("-d")) {
      readArg(args, new String[0]);
      String dataConfig = readArg(args, new String[] { "path/to/dataconfig.ttl" });
      try {
        List ep = EndpointFactory.loadFederationMembers(new File(dataConfig));
        this.endpoints.addAll(ep);
      } catch (FedXException e) {
        error("Data config '" + dataConfig + "' could not be loaded: " + e.getMessage(), false);
      }

    }
    else if (printError) {
      error("Expected at least one federation member (-s, -l, -d), was: " + arg, false);
    } else {
      return;
    }

    parseEndpoints(args, false);
  }

  protected void parseOutput(List<String> args)
  {
    String arg = null;
    if (!args.isEmpty()) {
      arg = (String)args.get(0);
    }
    if ((arg != null) && (arg.equals("-f"))) {
      readArg(args, new String[0]);

      String format = readArg(args, new String[] { "output format {STDOUT, XML, JSON}" });

      if (format.equals("STDOUT"))
        this.outputFormat = OutputFormat.STDOUT;
      else if (format.equals("JSON"))
        this.outputFormat = OutputFormat.JSON;
      else if (format.equals("XML")) {
        this.outputFormat = OutputFormat.XML;
      }
      else {
        error("Unexpected output format: " + format + ". Available options: STDOUT,XML,JSON", false);
      }

    }
    else if ((arg != null) && (arg.equals("-folder"))) {
      readArg(args, new String[0]);

      this.outFolder = readArg(args, new String[] { "outputFolder" });
    }
    else
    {
      return;
    }

    parseOutput(args);
  }

  protected void parseQueries(List<String> args)
  {
    String arg = (String)args.get(0);

    if (arg.equals("-q")) {
      readArg(args, new String[0]);
      String query = readArg(args, new String[] { "SparqlQuery" });
      this.queries.add(query);
    }
    else if (arg.equals("@q")) {
      readArg(args, new String[0]);
      String queryFile = readArg(args, new String[] { "path/to/queryFile" });
      try {
        List q = QueryStringUtil.loadQueries(queryFile);
        this.queries.addAll(q);
      } catch (IOException e) {
        error("Error loading query file '" + queryFile + "': " + e.getMessage(), false);
      }
    }
    else
    {
      error("Unexpected query argument: " + arg, false);
    }

    if (args.size() > 0)
      parseQueries(args);
  }

  protected String readArg(List<String> args, String[] expected)
  {
    if (args.size() == 0)
      error("Unexpected end of args, expected: " + expected, false);
    return (String)args.remove(0);
  }

  protected void initDefaultPrefixDeclarations()
  {
    QueryManager qm = FederationManager.getInstance().getQueryManager();
    Properties props = new Properties();
    try {
      props.load(SPARQLService.class.getResourceAsStream("/com/fluidops/fedx/commonPrefixesCli.prop"));
    } catch (IOException e) {
      throw new FedXRuntimeException("Error loading prefix properties: " + e.getMessage());
    }

    for (String ns : props.stringPropertyNames())
      qm.addPrefixDeclaration(ns, props.getProperty(ns));
  }

  protected List<String> runQuery(String queryString, int queryId) throws QueryEvaluationException, RepositoryException
  {
    TupleQuery query;
    try
    {
      query = QueryManager.prepareTupleQuery(queryString);
    } catch (MalformedQueryException e) {
      throw new QueryEvaluationException("Query is malformed: " + e.getMessage());
    }
    int count = 0;
    long start = System.currentTimeMillis();

    TupleQueryResult res = query.evaluate();
    List result = new ArrayList();

    while (res.hasNext()) {
      result.add(((BindingSet)res.next()).toString());

      count++;
    }

    res.close();

    long duration = System.currentTimeMillis() - start;

    System.out.println("Done query " + queryId + ": duration=" + duration + "ms, results=" + count);
    return result;
  }

  protected void error(String errorMsg, boolean printHelp)
  {
    System.out.println("ERROR: " + errorMsg);
    if (printHelp) {
      System.out.println("");
      printUsage(new boolean[0]);
    }
    System.exit(1);
  }

  protected void printUsage(boolean[] exit)
  {
    System.out.println("Usage:");
    System.out.println("> FedX [Configuration] [Federation Setup] [Output] [Queries]");
    System.out.println("> FedX -{help|version}");
    System.out.println("");
    System.out.println("WHERE");
    System.out.println("[Configuration] (optional)");

    System.out.println("Optionally specify the configuration to be used");
    System.out.println("\t-c path/to/fedxConfig");
    System.out.println("\t-verbose {0|1|2|3}");
    System.out.println("\t-logtofile");
    System.out.println("\t-p path/to/prefixDeclarations");
    System.out.println("");
    System.out.println("[Federation Setup] (optional)");
    System.out.println("Specify one or more federation members");
    System.out.println("\t-s urlToSparqlEndpoint");
    System.out.println("\t-l path/to/NativeStore");
    System.out.println("\t-d path/to/dataconfig.ttl");
    System.out.println("");
    System.out.println("[Output] (optional)");
    System.out.println("Specify the output options, default stdout. Files are created per query to results/%outputFolder%/q_%id%.{json|xml}, where the outputFolder is the current timestamp, if not specified otherwise.");
    System.out.println("\t-f {STDOUT,JSON,XML}");
    System.out.println("\t-folder outputFolder");
    System.out.println("");
    System.out.println("[Queries]");
    System.out.println("Specify one or more queries, in file: separation of queries by empty line");
    System.out.println("\t-q sparqlquery");
    System.out.println("\t@q path/to/queryfile");
    System.out.println("");
    System.out.println("Examples:");
    System.out.println("Please have a look at the examples attached to this package.");
    System.out.println("");
    System.out.println("Notes:");
    System.out.println("The federation members can be specified explicitely (-s,-l,-d) or implicitely as 'dataConfig' via the fedx configuration (-f)");
    System.out.println("If no PREFIX declarations are specified in the configurations, the CLI provides some common PREFIXES, currently rdf, rdfs and foaf. ");

    if ((exit.length != 0) && (exit[0] != false))
      System.exit(0);
  }

  protected void configureLogging()
  {
    Logger l = Logger.getLogger("com.fluidops.fedx");
    Logger rootLogger = Logger.getRootLogger();
    if (this.verboseLevel > 0)
    {
      if (this.verboseLevel == 1) {
        rootLogger.setLevel(Level.INFO);
        l.setLevel(Level.INFO);
      } else if (this.verboseLevel == 1) {
        rootLogger.setLevel(Level.DEBUG);
        l.setLevel(Level.DEBUG);
      } else if (this.verboseLevel > 2) {
        rootLogger.setLevel(Level.ALL);
        l.setLevel(Level.ALL);
      }

      if (this.logtofile)
        try {
          l.addAppender(new FileAppender(new PatternLayout("%5p %d{yyyy-MM-dd hh:mm:ss} [%t] (%F:%L) - %m%n"), "C:/logs/fedx_cli.log"));
        } catch (IOException e) {
          System.out.println("WARN: File Logging could not be initialized: " + e.getMessage());
        }
      else
        l.addAppender(new ConsoleAppender(new PatternLayout("%5p [%t] (%F:%L) - %m%n")));
    }
  }

  protected void configureRootLogger()
  {
    Logger rootLogger = Logger.getRootLogger();
    if (!rootLogger.getAllAppenders().hasMoreElements()) {
      rootLogger.setLevel(Level.ALL);
      rootLogger.addAppender(new NullAppender());
    }
  }

  protected void close() {
    try { FederationManager.getInstance().shutDown();
    } catch (FedXException e) {
      System.out.println("WARN: Federation could not be shut down: " + e.getMessage());
    }
  }

  protected static enum OutputFormat
  {
    STDOUT, JSON, XML;
  }
}