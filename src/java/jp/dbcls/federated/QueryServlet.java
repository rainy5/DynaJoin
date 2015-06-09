/*
 * Copyright (C) , DBCLS.ROIS.JP
 *
*/
package jp.dbcls.federated;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;

@WebServlet(name="QueryServlet", urlPatterns={"/result"})
public class QueryServlet extends HttpServlet
{
  SPARQLService myService;

  public void init()
    throws ServletException
  {
    super.init();
    String[] args = new String[5];
    args[0] = "-logtofile";
    args[1] = "-d";
    args[2] = "bio2rdf3.ttl";
    args[3] = "-f";
    args[4] = "STDOUT";
    this.myService = new SPARQLService();
    this.myService.run(args);
  }

  public void destory()
  {
    this.myService.close();
    super.destroy();
  }

  protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException, QueryEvaluationException, RepositoryException
  {
    String query = request.getParameter("query");

    List res = this.myService.runQuery(query, 0);

    response.setContentType("text/html;charset=UTF-8");

    PrintWriter out = response.getWriter();
    out.println("<!DOCTYPE html>");
    out.println("<html>");
    out.println("<head>");
    out.println("<meta content=\"text/html; charset=UTF-8\" http-equiv=\"Content-Type\"/>");
    out.println("<title>Bio2RDF Federated query</title>");

    out.println("</head>");
    out.println("<body>");

    out.println("The query:");

    out.println("<p/>");

    String temp = query.replace(">", "&gt;").replace("<", "&lt;");

    out.println(temp);
    System.out.println(temp);
    out.println("<p/>");

    out.println("The result:");
    out.println("<p/>");

    out.println(res);

    out.println("<p/>");

    out.println("<a href=\"/bio2RDF/\">return</a>");

    out.println("</body>");

    out.println("</html>");
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    try
    {
      processRequest(request, response);
    } catch (QueryEvaluationException ex) {
      Logger.getLogger(QueryServlet.class.getName()).log(Level.SEVERE, null, ex);
    } catch (RepositoryException ex) {
      Logger.getLogger(QueryServlet.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    doGet(request, response);
  }

  public String getServletInfo()
  {
    return "Short description";
  }
}