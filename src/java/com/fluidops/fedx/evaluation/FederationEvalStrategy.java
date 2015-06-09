/*
 * Copyright (C) 2008-2013, fluid Operations AG
 * Modified by Hongyan Wu@dbcls.rois
 *
 * FedX is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.fluidops.fedx.evaluation;

import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.FedX;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.CheckStatementPattern;
import com.fluidops.fedx.algebra.ConjunctiveFilterExpr;
import com.fluidops.fedx.algebra.EmptyResult;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.FedXService;
import com.fluidops.fedx.algebra.FedXStatementPattern;
import com.fluidops.fedx.algebra.FilterExpr;
import com.fluidops.fedx.algebra.IndependentJoinGroup;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.NUnion;
import com.fluidops.fedx.algebra.ProjectionWithBindings;
import com.fluidops.fedx.algebra.SingleSourceQuery;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.CacheUtils;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.evaluation.concurrent.ParallelServiceExecutor;
import com.fluidops.fedx.evaluation.iterator.QueryResultIter;
import com.fluidops.fedx.evaluation.union.ControlledWorkerUnion;
import com.fluidops.fedx.evaluation.union.ParallelGetStatementsTask;
import com.fluidops.fedx.evaluation.union.ParallelPreparedAlgebraUnionTask;
import com.fluidops.fedx.evaluation.union.ParallelPreparedUnionTask;
import com.fluidops.fedx.evaluation.union.ParallelUnionOperatorTask;
import com.fluidops.fedx.evaluation.union.SynchronousWorkerUnion;
import com.fluidops.fedx.evaluation.union.WorkerUnionBase;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.exception.IllegalQueryException;
import com.fluidops.fedx.statistics.Statistics;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.QueryType;
import com.fluidops.fedx.util.QueryStringUtil;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;
import info.aduna.iteration.SingletonIteration;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.federation.ServiceJoinIterator;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

public abstract class FederationEvalStrategy extends EvaluationStrategyImpl
{
  public static Logger log = Logger.getLogger(FederationEvalStrategy.class);
  protected Executor executor;
  protected Cache cache;
  protected Statistics statistics;
  private long optimizationEclipse = 0L;


  public FederationEvalStrategy() {
    super(new org.openrdf.query.algebra.evaluation.TripleSource()
    {
      public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, URI pred, Value obj, Resource[] contexts)
        throws QueryEvaluationException
      {
        throw new FedXRuntimeException(
          "Federation Strategy does not support org.openrdf.query.algebra.evaluation.TripleSource#getStatements. If you encounter this exception, please report it.");
      }

      public ValueFactory getValueFactory()
      {
        return ValueFactoryImpl.getInstance();
      }
    });
    this.executor = FederationManager.getInstance().getExecutor();
    this.cache = FederationManager.getInstance().getCache();
    this.statistics = FederationManager.getInstance().getStatistics();
  }
  public long getOptimizationTime() {
    return this.optimizationEclipse;
  }

  public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings)
    throws QueryEvaluationException
  {
    if ((expr instanceof StatementTupleExpr)) {
      return ((StatementTupleExpr)expr).evaluate(bindings);
    }

    if ((expr instanceof NJoin)) {
      try {
        return evaluateNJoin((NJoin)expr, bindings);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }

    if ((expr instanceof NUnion)) {
      return evaluateNaryUnion((NUnion)expr, bindings);
    }

    if ((expr instanceof ExclusiveGroup)) {
      return ((ExclusiveGroup)expr).evaluate(bindings);
    }

    if ((expr instanceof SingleSourceQuery)) {
      return evaluateSingleSourceQuery((SingleSourceQuery)expr, bindings);
    }
    if ((expr instanceof FedXService)) {
      return evaluateService((FedXService)expr, bindings);
    }

    if ((expr instanceof ProjectionWithBindings)) {
      return evaluateProjectionWithBindings((ProjectionWithBindings)expr, bindings);
    }

    if ((expr instanceof IndependentJoinGroup)) {
      return evaluateIndependentJoinGroup((IndependentJoinGroup)expr, bindings);
    }

    if ((expr instanceof EmptyResult)) {
      return new EmptyIteration();
    }
    return super.evaluate(expr, bindings);
  }

  public CloseableIteration<Statement, QueryEvaluationException> getStatements(QueryInfo queryInfo, Resource subj, URI pred, Value obj, Resource... contexts) throws RepositoryException, MalformedQueryException, QueryEvaluationException {

		if (contexts.length!=0)
			log.warn("Context queries are not yet supported by FedX.");
		
		List<Endpoint> members = FederationManager.getInstance().getFederation().getMembers();
		
		
		// a bound query: if at least one fed member provides results
		// return the statement, otherwise empty result
		if (subj!=null && pred!=null && obj!=null) {
			if (CacheUtils.checkCacheUpdateCache(cache, members, subj, pred, obj)) {
				return new SingletonIteration<Statement, QueryEvaluationException>(new StatementImpl(subj, pred, obj));
			}
			return new EmptyIteration<Statement, QueryEvaluationException>();
		}
		
		// form the union of results from relevant endpoints
		List<StatementSource> sources = CacheUtils.checkCacheForStatementSourcesUpdateCache(cache, members, subj, pred, obj);
		
		if (sources.size()==0)
			return new EmptyIteration<Statement, QueryEvaluationException>();
		
		if (sources.size()==1) {
			Endpoint e = EndpointManager.getEndpointManager().getEndpoint(sources.get(0).getEndpointID());
			return e.getTripleSource().getStatements(e.getConn(), subj, pred, obj, contexts);
		}
		
		// TODO why not collect in parallel?
		WorkerUnionBase<Statement> union = new SynchronousWorkerUnion<Statement>(queryInfo);		
		
		for (StatementSource source : sources) {
			Endpoint e = EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID());
			ParallelGetStatementsTask task = new ParallelGetStatementsTask(union, e.getTripleSource(), e.getConn(), subj, pred, obj, contexts);
			union.addTask(task);
		}
		
		// run the union in a separate thread
		executor.execute(union);
		
		// TODO distinct iteration ?
		
		return union;
	}
	
	
  public CloseableIteration<BindingSet, QueryEvaluationException> evaluateService(FedXService service, BindingSet bindings)
    throws QueryEvaluationException
  {
    ParallelServiceExecutor pe = new ParallelServiceExecutor(service, this, bindings);
    pe.run();
    return pe;
  }

  public CloseableIteration<BindingSet, QueryEvaluationException> evaluateSingleSourceQuery(SingleSourceQuery query, BindingSet bindings)
    throws QueryEvaluationException
  {
    try
    {
      Endpoint source = query.getSource();
      return source.getTripleSource().getStatements(query.getQueryString(), source.getConn(), query.getQueryInfo().getQueryType());
    } catch (RepositoryException e) {
      throw new QueryEvaluationException(e);
    } catch (MalformedQueryException e) {
      throw new QueryEvaluationException(e);
    }
  }

 
  public CloseableIteration<BindingSet, QueryEvaluationException> evaluateNaryUnion(NUnion union, BindingSet bindings)
    throws QueryEvaluationException
  {
    ControlledWorkerScheduler unionScheduler = FederationManager.getInstance().getUnionScheduler();
    ControlledWorkerUnion unionRunnable = new ControlledWorkerUnion(unionScheduler, union.getQueryInfo());

    for (int i = 0; i < union.getNumberOfArguments(); i++) {
      unionRunnable.addTask(new ParallelUnionOperatorTask(unionRunnable, this, union.getArg(i), bindings));
    }

    this.executor.execute(unionRunnable);

    return unionRunnable;
  }

  public abstract CloseableIteration<BindingSet, QueryEvaluationException> executeJoin(ControlledWorkerScheduler<BindingSet> paramControlledWorkerScheduler, CloseableIteration<BindingSet, QueryEvaluationException> paramCloseableIteration, TupleExpr paramTupleExpr, BindingSet paramBindingSet, QueryInfo paramQueryInfo)
    throws QueryEvaluationException;

  public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateExclusiveGroup(ExclusiveGroup paramExclusiveGroup, RepositoryConnection paramRepositoryConnection, TripleSource paramTripleSource, BindingSet paramBindingSet)
    throws RepositoryException, MalformedQueryException, QueryEvaluationException;

  public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateBoundJoinStatementPattern(StatementTupleExpr paramStatementTupleExpr, List<BindingSet> paramList)
    throws QueryEvaluationException;

  public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateGroupedCheck(CheckStatementPattern paramCheckStatementPattern, List<BindingSet> paramList)
    throws QueryEvaluationException;

  public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateIndependentJoinGroup(IndependentJoinGroup paramIndependentJoinGroup, BindingSet paramBindingSet)
    throws QueryEvaluationException;

  public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateIndependentJoinGroup(IndependentJoinGroup paramIndependentJoinGroup, List<BindingSet> paramList)
    throws QueryEvaluationException;

  public CloseableIteration<BindingSet, QueryEvaluationException> evaluateProjectionWithBindings(ProjectionWithBindings projection, BindingSet bindings)
    throws QueryEvaluationException
  {
    QueryBindingSet actualBindings = new QueryBindingSet(bindings);
    for (Binding b : projection.getAdditionalBindings())
      actualBindings.addBinding(b);
    return evaluate(projection, actualBindings);
  }

  public CloseableIteration<BindingSet, QueryEvaluationException> evaluateService(FedXService service, List<BindingSet> bindings)
    throws QueryEvaluationException
  {
    return new ServiceJoinIterator(new CollectionIteration(bindings), service.getService(), EmptyBindingSet.getInstance(), this);
  }

  public Value evaluate(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException
  {
    if ((expr instanceof FilterExpr))
      return evaluate((FilterExpr)expr, bindings);
    if ((expr instanceof ConjunctiveFilterExpr)) {
      return evaluate((ConjunctiveFilterExpr)expr, bindings);
    }
    return super.evaluate(expr, bindings);
  }

  public Value evaluate(FilterExpr node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException
  {
    Value v = evaluate(node.getExpression(), bindings);
    return BooleanLiteralImpl.valueOf(QueryEvaluationUtil.getEffectiveBooleanValue(v));
  }

  public Value evaluate(ConjunctiveFilterExpr node, BindingSet bindings)
    throws ValueExprEvaluationException, QueryEvaluationException
  {
    ValueExprEvaluationException error = null;

    for (FilterExpr expr : node.getExpressions()) {
      try
      {
        Value v = evaluate(expr.getExpression(), bindings);
        if (!QueryEvaluationUtil.getEffectiveBooleanValue(v))
          return BooleanLiteralImpl.FALSE;
      }
      catch (ValueExprEvaluationException e) {
        error = e;
      }
    }

    if (error != null) {
      throw error;
    }
    return BooleanLiteralImpl.TRUE;
  }

  public CloseableIteration<BindingSet, QueryEvaluationException> evaluateAtStatementSources(Object preparedQuery, List<StatementSource> statementSources, QueryInfo queryInfo)
    throws QueryEvaluationException
  {
    if ((preparedQuery instanceof String))
      return evaluateAtStatementSources((String)preparedQuery, statementSources, queryInfo);
    if ((preparedQuery instanceof TupleExpr))
      return evaluateAtStatementSources((TupleExpr)preparedQuery, statementSources, queryInfo);
    throw new RuntimeException("Unsupported type for prepared query: " + preparedQuery.getClass().getCanonicalName());
  }

  protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateAtStatementSources(String preparedQuery, List<StatementSource> statementSources, QueryInfo queryInfo) throws QueryEvaluationException {
		
		try {
			CloseableIteration<BindingSet, QueryEvaluationException> result;
			
			if (statementSources.size()==1) {				
				Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(statementSources.get(0).getEndpointID());
				RepositoryConnection conn = ownedEndpoint.getConn();
				com.fluidops.fedx.evaluation.TripleSource t = ownedEndpoint.getTripleSource();
				result = t.getStatements(preparedQuery, conn, EmptyBindingSet.getInstance(), null);
			} 
			 
			else {			
				WorkerUnionBase<BindingSet> union = FederationManager.getInstance().createWorkerUnion(queryInfo);
				
				for (StatementSource source : statementSources) {					
					Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID());
					RepositoryConnection conn = ownedEndpoint.getConn();
					com.fluidops.fedx.evaluation.TripleSource t = ownedEndpoint.getTripleSource();
					union.addTask(new ParallelPreparedUnionTask(union, preparedQuery, t, conn, EmptyBindingSet.getInstance(), null));
				}
				
				union.run();				
				result = union;
				
				
				// TODO we should add some DISTINCT here to have SET semantics
			}
		
			return result;
			
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}
	}
	
	
  protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateAtStatementSources(TupleExpr preparedQuery, List<StatementSource> statementSources, QueryInfo queryInfo) throws QueryEvaluationException {
		
		try {
			CloseableIteration<BindingSet, QueryEvaluationException> result;
			
			if (statementSources.size()==1) {				
				Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(statementSources.get(0).getEndpointID());
				RepositoryConnection conn = ownedEndpoint.getConn();
				com.fluidops.fedx.evaluation.TripleSource t = ownedEndpoint.getTripleSource();
				result = t.getStatements(preparedQuery, conn, EmptyBindingSet.getInstance(), null);
			} 
			 
			else {			
				WorkerUnionBase<BindingSet> union = FederationManager.getInstance().createWorkerUnion(queryInfo);
				
				for (StatementSource source : statementSources) {					
					Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID());
					RepositoryConnection conn = ownedEndpoint.getConn();
					com.fluidops.fedx.evaluation.TripleSource t = ownedEndpoint.getTripleSource();
					union.addTask(new ParallelPreparedAlgebraUnionTask(union, preparedQuery, t, conn, EmptyBindingSet.getInstance(), null));					
				}
				
				union.run();				
				result = union;
				
				// TODO we should add some DISTINCT here to have SET semantics
			}
		
			return result;
			
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}
	}
  
  private Pair findMinumCostfromAllTriplePattern(List<TupleExpr> tupleExpr, CloseableIteration<BindingSet, QueryEvaluationException> preResultIter)
    throws Exception
  {
    List preResult = new ArrayList();

    while ((preResultIter != null) && (preResultIter.hasNext()))
      preResult.add((BindingSet)preResultIter.next());
    CloseableIteration clone = new QueryResultIter(preResult);
    long minmum = Long.MAX_VALUE;
    int index = 0;

    int heuristicNum = 3;

    int size = 0;

    size = preResult.size();

    long temp = Long.MAX_VALUE;

    for (int i = 0; i < tupleExpr.size(); i++)
    {
      TupleExpr exp = (TupleExpr)tupleExpr.get(i);
      String countQuery = constructCountQuery(exp, preResult, heuristicNum);
      System.out.println("countQuery:" + countQuery);
      log.info("countQuery:" + countQuery);
      temp = evaluateCountQuery(exp, countQuery, size, heuristicNum);
      if (temp < minmum) {
        minmum = temp;
        index = i;
      }
      log.info(" countQuery result:" + temp);
    }

    return new Pair(index, clone);
  }


  private long evaluateCountQuery(TupleExpr exp, String countQuery, int size, int heuristicNum)
    throws Exception
  {
    CloseableIteration result = null;
  
    long value = Integer.MAX_VALUE;

    if ((exp instanceof ExclusiveGroup))
    {
      StatementSource source = ((ExclusiveGroup)exp).getOwner();
      String endpointID = source.getEndpointID();
      Endpoint endpoint = EndpointManager.getEndpointManager().getEndpoint(endpointID);
      result = endpoint.getTripleSource().getStatements(countQuery, endpoint.getConn(), QueryType.SELECT);
      BindingSet binding = (BindingSet)result.next();
      result.close();
      value = Integer.parseInt(binding.getValue("callret-0").stringValue());
      return value;
    }

    QueryInfo qInfo = new QueryInfo(countQuery, QueryType.SELECT);
    List sources = ((FedXStatementPattern)exp).getStatementSources();
    result = evaluateAtStatementSources(countQuery, sources, qInfo);
    long sum=0;
    if(result.hasNext()){
    while(result.hasNext()){
    BindingSet binding = (BindingSet)result.next();
    String temp=binding.getValue("callret-0").stringValue();
    if (temp.length()>9) 
    	 sum = sum+Integer.MAX_VALUE;
    else
       sum = sum+Integer.parseInt(temp);
    }
    value=sum;
    }  
    result.close();   
    return value;
  }

  private String constructCountQuery(TupleExpr tupleExpr, List<BindingSet> preResult, int heuristicNum)
    throws IllegalQueryException
  {
    List temp = new ArrayList();

    if (preResult != null) {
      int size = preResult.size();

      if ((heuristicNum == 0) || (heuristicNum >= size)) {
        temp.addAll(preResult);
      } else {
        int dis = size / heuristicNum;
        for (int i = 0; i < heuristicNum; i++) {
          temp.add((BindingSet)preResult.get(i * dis));
        }

      }

    }

    String countString = QueryStringUtil.selectCountQueryString(tupleExpr, temp, null, Boolean.valueOf(false));

    return countString;
  }
 
		  public CloseableIteration<BindingSet, QueryEvaluationException> evaluateNJoin(NJoin join, BindingSet bindings) throws Exception {
		    long start = 0L;
		    long eclipsed = 0L;
		    long sum = 0L;

		    boolean isJoin = false;
		     ControlledWorkerScheduler joinScheduler;
		     CloseableIteration<BindingSet, QueryEvaluationException> clone;
		     CloseableIteration<BindingSet, QueryEvaluationException> preResult = null;
		    List temp = null;
		    joinScheduler = FederationManager.getInstance().getJoinScheduler();
		    List tupleExpr = join.getArgs();
		    while (!tupleExpr.isEmpty())
		    {
		      int minIndex = 0;
		      start = System.currentTimeMillis();
		      clone = null;
		      Pair pair = null;
		      if (tupleExpr.size()>1) {
		        pair = findMinumCostfromAllTriplePattern(tupleExpr, preResult);
		        minIndex = pair.index;
		        clone = pair.clone; } 
                      else {
		        clone = preResult;
		      }eclipsed = System.currentTimeMillis() - start;
		      sum += eclipsed;

		      TupleExpr curExpr = (TupleExpr)tupleExpr.get(minIndex);
		      System.out.println("The evaluated pattern:" + curExpr);

		      log.info("The evaluated pattern:" + curExpr);
		      
		      if(!isJoin)
		      {
		      preResult = evaluate(curExpr, bindings);
		      isJoin=true;
		      }
		      else
		      preResult = executeJoin(joinScheduler, clone, curExpr, bindings, join.getQueryInfo());

		      tupleExpr.remove(minIndex);

		      tupleExpr = join.getArgs();
		    }

		    this.optimizationEclipse = sum;
		    log.info("Optimization2 duration(ms): " + this.optimizationEclipse);
		    System.out.println("Optimization2 duration(ms): " + this.optimizationEclipse);

		    return preResult;
		  }
	
	
  private class Pair
  {
    int index;
    CloseableIteration<BindingSet, QueryEvaluationException> clone;

    Pair(int index, CloseableIteration<BindingSet, QueryEvaluationException>  clone)
    {
      this.index = index;
      this.clone = clone;
    }
  }
}