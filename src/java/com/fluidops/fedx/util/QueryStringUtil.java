package com.fluidops.fedx.util;

import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.FedXStatementPattern;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.algebra.IndependentJoinGroup;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.exception.FilterConversionException;
import com.fluidops.fedx.exception.IllegalQueryException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.EmptyBindingSet;

public class QueryStringUtil
{
  public static Logger log = Logger.getLogger(QueryStringUtil.class);

  public static final URI BNODE_URI = ValueFactoryImpl.getInstance().createURI("http://fluidops.com/fedx/bnode");

  public static boolean hasFreeVars(StatementPattern stmt, BindingSet bindings)
  {
    for (Var var : stmt.getVarList()) {
      if ((!var.hasValue()) && (!bindings.hasBinding(var.getName())))
        return true;
    }
    return false;
  }

  public static String toString(StatementPattern stmt)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    appendVar(sb, stmt.getSubjectVar(), new HashSet(), EmptyBindingSet.getInstance());
    sb.append("; ");
    appendVar(sb, stmt.getPredicateVar(), new HashSet(), EmptyBindingSet.getInstance());
    sb.append("; ");
    appendVar(sb, stmt.getObjectVar(), new HashSet(), EmptyBindingSet.getInstance());
    sb.append("}");
    return sb.toString();
  }

  public static String toString(Var var) {
    if (!var.hasValue())
      return "?" + var.getName();
    return getValueString(var.getValue());
  }

  public static String toString(Resource subj, URI pred, Value obj)
  {
    return toString(QueryAlgebraUtil.toStatementPattern(subj, pred, obj));
  }

  public static String selectQueryString( FedXStatementPattern stmt, BindingSet bindings, FilterValueExpr filterExpr, Boolean evaluated) throws IllegalQueryException {
		
		Set<String> varNames = new HashSet<String>();
		String s = constructStatement(stmt, varNames, bindings);
		
		// project only relevant variables, i.e. do not bind local variables
		varNames = project(stmt, varNames);
		
		StringBuilder res = new StringBuilder();
		
		res.append("SELECT ");
		
		if (varNames.size()==0)
			throw new IllegalQueryException("SELECT query needs at least one projection!");
		
		for (String var : varNames)
			res.append(" ?").append(var);
		
		res.append(" WHERE { ").append(s);
		
		if (filterExpr!=null) {
			try {
				String filter = FilterUtils.toSparqlString(filterExpr);
				res.append("FILTER ").append(filter);
				evaluated = true;
			} catch (FilterConversionException e) {
				log.warn("Filter could not be evaluated remotely. " + e.getMessage());
			}
		}
	
		res.append(" }");
		
		return res.toString();		
	}
	
  public static String selectCountQueryString(TupleExpr expr, List<BindingSet> bindings, FilterValueExpr filterExpr, Boolean evaluated)
    throws IllegalQueryException
  {
    String result = null;

    if ((bindings == null) || (bindings.size() == 0)) {
      if ((expr instanceof ExclusiveGroup))
      {
        filterExpr = ((ExclusiveGroup)expr).getFilterExpr();
        result = selectCountQueryString((ExclusiveGroup)expr, EmptyBindingSet.getInstance(), filterExpr, evaluated);
      }

      if ((expr instanceof FedXStatementPattern))
      {
        filterExpr = ((FedXStatementPattern)expr).getFilterExpr();
        result = selectCountQueryString((FedXStatementPattern)expr, EmptyBindingSet.getInstance(), filterExpr, evaluated);
      }
    }
    else {
      if ((expr instanceof ExclusiveGroup))
      {
        filterExpr = ((ExclusiveGroup)expr).getFilterExpr();
        result = selectCountQueryString((ExclusiveGroup)expr, bindings, filterExpr, evaluated);
      }

      if ((expr instanceof FedXStatementPattern))
      {
        filterExpr = ((FedXStatementPattern)expr).getFilterExpr();
        result = selectCountQueryString((FedXStatementPattern)expr, bindings, filterExpr, evaluated);
      }
    }

    return result;
  }

  public static String selectQueryString(TupleExpr expr, BindingSet bindings, FilterValueExpr filterExpr, Boolean evaluated) throws IllegalQueryException {
    String result = null;
    if ((expr instanceof ExclusiveGroup))
    {
      result = selectQueryString((ExclusiveGroup)expr, bindings, filterExpr, evaluated);
    }
    if ((expr instanceof FedXStatementPattern))
    {
      result = selectQueryString((FedXStatementPattern)expr, bindings, filterExpr, evaluated);
    }
    return result;
  }

  private static String selectQueryString(ExclusiveGroup group, BindingSet bindings, FilterValueExpr filterExpr, Boolean evaluated) throws IllegalQueryException
  {
    StringBuilder sb = new StringBuilder();
    Set<String> varNames = new HashSet();

    for (ExclusiveStatement s : group.getStatements()) {
      sb.append(constructStatement(s, varNames, bindings));
    }

    varNames = project(group, varNames);

    if (varNames.size() == 0) {
      throw new IllegalQueryException("SELECT query needs at least one projection!");
    }
    StringBuilder res = new StringBuilder();
    res.append("SELECT  ");

    for (String var : varNames) {
      res.append(" ?").append(var);
    }

    res.append(" WHERE { ").append(sb);
    filterExpr = group.getFilterExpr();
    if (filterExpr != null) {
      try {
        String filter = FilterUtils.toSparqlString(filterExpr);
        res.append("FILTER ").append(filter);
        evaluated = Boolean.valueOf(true);
      } catch (FilterConversionException e) {
        log.warn("Filter could not be evaluated remotely. " + e.getMessage());
      }
    }

    res.append(" }");

    return res.toString();
  }

  private static String selectCountQueryString(FedXStatementPattern stmt, List<BindingSet> bindingsList, FilterValueExpr filterExpr, Boolean evaluated) throws IllegalQueryException
  {
    Set varNames = new HashSet();

    StringBuilder s = new StringBuilder();
    int length = bindingsList.size();
    int cnt = 1;
    Set share = new HashSet();
    share.addAll(stmt.getBindingNames());
    for (BindingSet bindings : bindingsList) {
      s.append("{");
      s.append(constructStatement(stmt, varNames, bindings));
      s.append("}");

      if (!checkContain(share, bindings.getBindingNames()))
        break;
      if (cnt < length) { s.append(" union ");
        cnt++;
      }

    }

    varNames = project(stmt, varNames);

    StringBuilder res = new StringBuilder();

    res.append("SELECT count(*) ");

    res.append(" WHERE { ").append(s);

    res.append(" }");

    return res.toString();
  }

  public static String selectTypeQueryString(FedXStatementPattern stmt, BindingSet bindings, FilterValueExpr filterExpr, Boolean evaluated, String name) throws IllegalQueryException {
    Set varNames = new HashSet();
    String s = constructStatement(stmt, varNames, bindings);

    varNames = project(stmt, varNames);

    StringBuilder res = new StringBuilder();

    res.append("SELECT ?type ");

    res.append(" WHERE { ").append(s);
    res.append(name).append(" a ?type.");

    if (filterExpr != null) {
      try {
        String filter = FilterUtils.toSparqlString(filterExpr);
        res.append("FILTER ").append(filter);
        evaluated = Boolean.valueOf(true);
      } catch (FilterConversionException e) {
        log.warn("Filter could not be evaluated remotely. " + e.getMessage());
      }
    }

    res.append(" }");

    return res.toString();
  }

  private static String selectCountQueryString(FedXStatementPattern stmt, BindingSet bindings, FilterValueExpr filterExpr, Boolean evaluated) throws IllegalQueryException
  {
    Set varNames = new HashSet();
    String s = constructStatement(stmt, varNames, bindings);

    varNames = project(stmt, varNames);

    StringBuilder res = new StringBuilder();

    res.append("SELECT count(*) ");

    res.append(" WHERE { ").append(s);

    res.append(" }");

    return res.toString();
  }

  private static String selectCountQueryString(ExclusiveGroup group, BindingSet bindings, FilterValueExpr filterExpr, Boolean evaluated) throws IllegalQueryException
  {
    StringBuilder sb = new StringBuilder();
    Set varNames = new HashSet();

    for (ExclusiveStatement s : group.getStatements()) {
      sb.append(constructStatement(s, varNames, bindings));
    }

    varNames = project(group, varNames);

    StringBuilder res = new StringBuilder();
    res.append("SELECT count(*) ");

    res.append(" WHERE { ").append(sb);

    res.append(" }");

    return res.toString();
  }

  private static boolean checkContain(Set<String> set1, Set<String> set2)
  {
    Iterator localIterator2;
    for (Iterator localIterator1 = set1.iterator(); localIterator1.hasNext(); 
      localIterator2.hasNext())
    {
      String mother = (String)localIterator1.next();
      localIterator2 = set2.iterator();
   //   continue; 
      String child = (String)localIterator2.next();
      if (mother.equals(child)) {
        return true;
      }
    }
    return false;
  }

  private static String selectCountQueryString(ExclusiveGroup group, List<BindingSet> bindingsList, FilterValueExpr filterExpr, Boolean evaluated) throws IllegalQueryException
  {
    StringBuilder sb = new StringBuilder();
    Set varNames = new HashSet();
    int length = bindingsList.size();
    int cnt = 1;
    Set share = new HashSet();
    for (BindingSet bindings : bindingsList)
    {
      sb.append("{");
      for (ExclusiveStatement s : group.getStatements()) {
        share.addAll(s.getBindingNames());
        sb.append(constructStatement(s, varNames, bindings));
      }
      sb.append("}");

      if (!checkContain(share, bindings.getBindingNames()))
        break;
      if (cnt < length) { sb.append(" union ");
        cnt++;
      }

    }

    varNames = project(group, varNames);

    StringBuilder res = new StringBuilder();
    res.append("SELECT count(*) ");

    res.append(" WHERE { ").append(sb);

    res.append(" }");

    return res.toString();
  }

  public static String askQueryString(ExclusiveGroup group, BindingSet bindings)
  {
    StringBuilder sb = new StringBuilder();
    Set varNames = new HashSet();

    for (ExclusiveStatement s : group.getStatements()) {
      sb.append(constructStatement(s, varNames, bindings));
    }
    StringBuilder res = new StringBuilder();
    res.append("ASK { ").append(sb.toString()).append(" }");
    return res.toString();
  }

  public static String selectQueryStringBoundUnion(StatementPattern stmt, List<BindingSet> unionBindings, FilterValueExpr filterExpr, Boolean evaluated)
  {
    Set<String> varNames = new HashSet();

    StringBuilder unions = new StringBuilder();
    for (int i = 0; i < unionBindings.size(); i++) {
      String s = constructStatementId(stmt, Integer.toString(i), varNames, (BindingSet)unionBindings.get(i));
      if (i > 0)
        unions.append(" UNION");
      unions.append(" { ").append(s).append(" }");
    }

    StringBuilder res = new StringBuilder();

    res.append("SELECT ");

    for (String var : varNames) {
      res.append(" ?").append(var);
    }
    res.append(" WHERE {");

    res.append(unions);

    res.append(" }");

    return res.toString();
  }

  public static String selectQueryStringBoundJoinVALUES(StatementPattern stmt, List<BindingSet> unionBindings, FilterValueExpr filterExpr, Boolean evaluated)
  {
    Set<String> varNames = new LinkedHashSet();
    StringBuilder res = new StringBuilder();

    String stmtPattern = constructStatement(stmt, varNames, new EmptyBindingSet());
    res.append("SELECT ");

    for (String var : varNames) {
      res.append(" ?").append(var);
    }
    res.append(" ?").append("__index").append(" WHERE {");

    res.append(stmtPattern);

    res.append(" }");

    res.append(" VALUES (");

    for (String var : varNames)
      res.append("?").append(var).append(" ");
    res.append(" ?__index) { ");

    int index = 0;
    for (BindingSet b : unionBindings) {
      res.append("(");
      for (String var : varNames) {
        if (b.hasBinding(var))
          appendValue(res, b.getValue(var)).append(" ");
        else
          res.append("UNDEF ");
      }
      res.append("\"").append(index).append("\") ");
      index++;
    }
    res.append(" }");

    return res.toString();
  }

  private static Set<String> project(StatementTupleExpr expr, Set<String> varNames)
  {
    varNames.removeAll(expr.getLocalVars());
    return varNames;
  }

  public static String selectQueryStringBoundCheck(StatementPattern stmt, List<BindingSet> unionBindings)
  {
    Set<String> varNames = new HashSet();

    StringBuilder unions = new StringBuilder();
    for (int i = 0; i < unionBindings.size(); i++) {
      String s = constructStatementCheckId(stmt, i, varNames, (BindingSet)unionBindings.get(i));
      if (i > 0)
        unions.append(" UNION");
      unions.append(" { ").append(s).append(" }");
    }

    StringBuilder res = new StringBuilder();

    res.append("SELECT ");

    for (String var : varNames) {
      res.append(" ?").append(var);
    }
    res.append(" WHERE {").append(unions).append(" }");

    return res.toString();
  }

  public static String askQueryStringBoundCheck(StatementPattern stmt, List<BindingSet> unionBindings)
  {
    Set<String> varNames = new HashSet();

    StringBuilder unions = new StringBuilder();

    StringBuilder res = new StringBuilder();

    res.append("Ask {");

    int i = 0;

    BindingSet binding = (BindingSet)unionBindings.get(i);

    Set bindName = binding.getBindingNames();

    String temp = stmt.getSubjectVar().getName();
    Value stValue = stmt.getSubjectVar().getValue();
    if (bindName.contains(temp)) {
      Value value = binding.getValue(temp);
      appendValue(res, value);
    }
    else if (stValue != null) { appendValue(res, stValue);
    } else {
      res.append("?").append(temp);
    }res.append(" ");

    temp = stmt.getPredicateVar().getName();
    stValue = stmt.getPredicateVar().getValue();
    if (bindName.contains(temp)) {
      Value value = binding.getValue(temp);
      appendValue(res, value);
    }
    else if (stValue != null) { appendValue(res, stValue);
    } else {
      res.append("?").append(temp);
    }res.append(" ");

    temp = stmt.getObjectVar().getName();
    stValue = stmt.getObjectVar().getValue();
    if (bindName.contains(temp)) {
      Value value = binding.getValue(temp);
      appendValue(res, value);
    }
    else if (stValue != null) { appendValue(res, stValue);
    } else {
      res.append("?").append(temp);
    }res.append(". ");

    res.append("}");

    return res.toString();
  }

  public static String selectQueryStringIndependentJoinGroup(IndependentJoinGroup joinGroup, BindingSet bindings)
  {
    Set<String> varNames = new HashSet();

    StringBuilder unions = new StringBuilder();
    String s;
    for (int i = 0; i < joinGroup.getMemberCount(); i++) {
      StatementPattern stmt = (StatementPattern)joinGroup.getMembers().get(i);
      s = constructStatementId(stmt, Integer.toString(i), varNames, bindings);
      if (i > 0)
        unions.append(" UNION");
      unions.append(" { ").append(s).append(" }");
    }

    StringBuilder res = new StringBuilder();

    res.append("SELECT ");

    for (String var : varNames) {
      res.append(" ?").append(var);
    }
    res.append(" WHERE {");

    res.append(unions);

    res.append(" }");

    return res.toString();
  }

  public static String selectQueryStringIndependentJoinGroup(IndependentJoinGroup joinGroup, List<BindingSet> bindings)
  {
    Set<String> varNames = new HashSet();

    StringBuilder outerUnion = new StringBuilder();
    for (int i = 0; i < joinGroup.getMemberCount(); i++) {
      String innerUnion = constructInnerUnion((StatementPattern)joinGroup.getMembers().get(i), i, varNames, bindings);
      if (i > 0)
        outerUnion.append(" UNION");
      outerUnion.append(" { ").append(innerUnion).append("}");
    }

    StringBuilder res = new StringBuilder();

    res.append("SELECT ");

    for (String var : varNames) {
      res.append(" ?").append(var);
    }
    res.append(" WHERE {");

    res.append(outerUnion);

    res.append(" }");

    return res.toString();
  }

  protected static String constructInnerUnion(StatementPattern stmt, int outerID, Set<String> varNames, List<BindingSet> bindings)
  {
    StringBuilder innerUnion = new StringBuilder();

    for (int idx = 0; idx < bindings.size(); idx++) {
      if (idx > 0)
        innerUnion.append("UNION ");
      innerUnion.append("{").append(constructStatementId(stmt, outerID + "_" + idx, varNames, (BindingSet)bindings.get(idx))).append("} ");
    }

    return innerUnion.toString();
  }

  public static String askQueryString(StatementPattern stmt, BindingSet bindings)
  {
    Set varNames = new HashSet();
    String s = constructStatement(stmt, varNames, bindings);

    StringBuilder res = new StringBuilder();

    res.append("ASK {");
    res.append(s).append(" }");

    return res.toString();
  }

  public static String selectQueryStringLimit1(StatementPattern stmt, BindingSet bindings)
  {
    Set varNames = new HashSet();
    String s = constructStatement(stmt, varNames, bindings);

    StringBuilder res = new StringBuilder();

    res.append("SELECT * WHERE {");
    res.append(s).append(" } LIMIT 1");

    return res.toString();
  }

  public static String selectQueryStringLimit1(ExclusiveGroup group, BindingSet bindings)
  {
    Set varNames = new HashSet();
    StringBuilder res = new StringBuilder();

    res.append("SELECT * WHERE { ");

    for (ExclusiveStatement s : group.getStatements()) {
      res.append(constructStatement(s, varNames, bindings));
    }
    res.append(" } LIMIT 1");

    return res.toString();
  }

  protected static String constructStatement(StatementPattern stmt, Set<String> varNames, BindingSet bindings)
  {
    StringBuilder sb = new StringBuilder();

    sb = appendVar(sb, stmt.getSubjectVar(), varNames, bindings).append(" ");
    sb = appendVar(sb, stmt.getPredicateVar(), varNames, bindings).append(" ");
    sb = appendVar(sb, stmt.getObjectVar(), varNames, bindings).append(" . ");

    return sb.toString();
  }

  protected static String constructStatementId(StatementPattern stmt, String varID, Set<String> varNames, BindingSet bindings)
  {
    StringBuilder sb = new StringBuilder();

    sb = appendVarId(sb, stmt.getSubjectVar(), varID, varNames, bindings).append(" ");
    sb = appendVarId(sb, stmt.getPredicateVar(), varID, varNames, bindings).append(" ");
    sb = appendVarId(sb, stmt.getObjectVar(), varID, varNames, bindings).append(" . ");

    return sb.toString();
  }

  /**
	 * Construct the statement string, i.e. "s p ?o_varID FILTER ?o_N=o ". This kind of statement
	 * pattern is necessary to later on identify available results.
	 * 
	 * @param stmt
	 * @param varID
	 * @param varNames
	 * @param bindings
	 * @return
	 */
	protected static String constructStatementCheckId(StatementPattern stmt, int varID, Set<String> varNames, BindingSet bindings) {
		StringBuilder sb = new StringBuilder();
		
		String _varID = Integer.toString(varID);
		sb = appendVarId(sb, stmt.getSubjectVar(), _varID, varNames, bindings).append(" ");
		sb = appendVarId(sb, stmt.getPredicateVar(), _varID, varNames, bindings).append(" ");
		
		sb.append("?o_").append(_varID);
		varNames.add("o_" + _varID);
		
		String objValue;
		if (stmt.getObjectVar().hasValue()) {
			objValue = getValueString(stmt.getObjectVar().getValue());
		} else if (bindings.hasBinding(stmt.getObjectVar().getName())){
			objValue = getValueString(bindings.getBinding(stmt.getObjectVar().getName()).getValue());
		} else {
			// just to make sure that we see an error, will be deleted soon
			throw new RuntimeException("Unexpected.");
		}
		
		sb.append(" FILTER (?o_").append(_varID).append(" = ").append(objValue).append(" )");
				
		return sb.toString();
	}
	
  protected static StringBuilder appendVar(StringBuilder sb, Var var, Set<String> varNames, BindingSet bindings)
  {
    if (!var.hasValue()) {
      if (bindings.hasBinding(var.getName()))
        return appendValue(sb, bindings.getValue(var.getName()));
      varNames.add(var.getName());
      return sb.append("?").append(var.getName());
    }

    return appendValue(sb, var.getValue());
  }

  protected static StringBuilder appendVarId(StringBuilder sb, Var var, String varID, Set<String> varNames, BindingSet bindings)
  {
    if (!var.hasValue()) {
      if (bindings.hasBinding(var.getName()))
        return appendValue(sb, bindings.getValue(var.getName()));
      String newName = var.getName() + "_" + varID;
      varNames.add(newName);
      return sb.append("?").append(newName);
    }

    return appendValue(sb, var.getValue());
  }

  protected static String getValueString(Value value)
  {
    StringBuilder sb = new StringBuilder();
    appendValue(sb, value);
    return sb.toString();
  }

  protected static StringBuilder appendValue(StringBuilder sb, Value value)
  {
    if ((value instanceof URI))
      return appendURI(sb, (URI)value);
    if ((value instanceof Literal))
      return appendLiteral(sb, (Literal)value);
    if ((value instanceof BNode))
      return appendBNode(sb, (BNode)value);
    throw new RuntimeException("Type not supported: " + value.getClass().getCanonicalName());
  }

  protected static StringBuilder appendURI(StringBuilder sb, URI uri)
  {
    sb.append("<").append(uri.stringValue()).append(">");
    return sb;
  }

  protected static StringBuilder appendBNode(StringBuilder sb, BNode bNode)
  {
    log.debug("Cannot express BNodes in SPARQl: Bnode " + bNode.toString() + " is replaced with " + BNODE_URI.stringValue());

    return appendURI(sb, BNODE_URI);
  }

  protected static StringBuilder appendLiteral(StringBuilder sb, Literal lit)
  {
    sb.append('"');
    sb.append(lit.getLabel().replace("\"", "\\\""));
    sb.append('"');

    if (lit.getLanguage() != null) {
      sb.append('@');
      sb.append(lit.getLanguage());
    }

    if (lit.getDatatype() != null) {
      sb.append("^^<");
      sb.append(lit.getDatatype().stringValue());
      sb.append('>');
    }
    return sb;
  }

	/**
	 * load the queries from a queries file located at the specified path.
	 * 
	 * Expected format:
	 *  - Queries are SPARQL queries in String format
	 *  - queries are allowed to span several lines
	 *  - a query is interpreted to be finished if an empty line occurs
	 *  
	 *  Ex:
	 *  
	 *  QUERY1 ...
	 *   Q1 cntd
	 *   
	 *  QUERY2
	 * 
	 * @param queryType
	 * @return
	 * 			a list of queries for the query type
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static List<String> loadQueries(String queryFile) throws FileNotFoundException, IOException {
		ArrayList<String> res = new ArrayList<String>();
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(queryFile));
			String tmp;
			String tmpQuery = "";
			while ((tmp = in.readLine()) != null){
				if (tmp.equals("")){
					if (!tmpQuery.equals(""))
						res.add(tmpQuery);
					tmpQuery = "";
				}
				else {
					tmpQuery = tmpQuery + tmp;
				}
			}
			if (!tmpQuery.equals(""))
				res.add(tmpQuery);
			return res;
		} finally {
			if (in!=null)
				in.close();
		}
		
	}
}