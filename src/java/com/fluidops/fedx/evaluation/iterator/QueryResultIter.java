package com.fluidops.fedx.evaluation.iterator;

import info.aduna.iteration.CloseableIterationBase;
import java.util.Iterator;
import java.util.List;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

public class QueryResultIter extends CloseableIterationBase<BindingSet, QueryEvaluationException>
{
  protected final List<BindingSet> inner;
  Iterator<BindingSet> it;

  public QueryResultIter(List<BindingSet> inner)
  {
    this.inner = inner;
    this.it = inner.iterator();
  }

  public boolean hasNext()
    throws QueryEvaluationException
  {
    synchronized (this) {
      if (this.it.hasNext()) {
        return true;
      }
      return false;
    }
  }

  public BindingSet next()
    throws QueryEvaluationException
  {
    synchronized (this) {
      BindingSet next = (BindingSet)this.it.next();
      return next;
    }
  }

  public void remove() throws QueryEvaluationException
  {
    this.it.remove();
  }

  protected void handleClose()
    throws QueryEvaluationException
  {
    abortQuery();
  }

  protected void abortQuery()
  {
  }
}