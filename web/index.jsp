<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Federated query service DynaJoin</title>
        <link href="assets/application.css" 
              media="all" rel="stylesheet" type="text/css" />
        <link href="assets/jquery.dataTables.min.css" 
              media="all" rel="stylesheet" type="text/css" />
        <script src="assets/example.js" type="text/javascript"></script>      
        <script src="assets/query.js" type="text/javascript"></script>        
         <script src="assets/jquery-1.11.3.min.js" type="text/javascript"></script>
        <script src="assets/jquery.dataTables.min.js" type="text/javascript"></script>
         <script src="assets/codemirror.js" type="text/javascript"></script>
        <script src="assets/sparql.js" type="text/javascript"></script> 
    </head>
    <body>
         <div>
            <div id="header" class="top col-1-2">
                <div class="title">DynaJoin SPARQL Interface</div>
                <div class="subtitle">Federated query service via SPARQL endpoints</div>
            </div>
            <div id="nav" class="top col-1-2">
                <a href="index.jsp">Home</a>|
                <a href="documentation.html">Documentation</a> |
                <a href="data.html">Data</a> |
                  <a href="about.html">About</a> 
            </div>
        </div>   
        <div id="main">
            <fieldset style="display:inline-block">
                <legend>Enter your SPARQL query and endpoints</legend>
                <form id="queryform" name="sparql" method="get" action="" >
                    <div>
                        <textarea id="query" name="query"  style="height:160px; width:680px"></textarea>
                    </div>
                    <br/>
                    <div>
                        <textarea id="endpoints" name="endpoints"  style="height:160px; width:596px"></textarea>
                    </div>
                    <select id="endpoint" onchange="changeEndpint()">
                        <option value="0">Configure endpoint</option>
                        <option value="1">Bio2RDF</option>
                        <option value="2">Self defined</option>
                    </select>
                    <input  type="submit" value="Submit Query"  style="height: 30px; width: 460px" onclick="onSparqlQuery(event)" >
                    <div>
                        <a id="resultLink"> Download the results in JSON</a>
                    </div>
                </form>              
            </fieldset>
            <div class="example-queries">
                <h2><span>Example queries</span></h2>
                <table class="query-list" id="exampleQueryTable">
                    <tr>
                        <th>No.</th>
                        <th>Title</th>
                    </tr>
                </table>
            </div>
            <div id="query-result">
                <table id="resultTable" class="display" width="100%"></table>
            </div>
        </div>
     <script>
          var dataTable,
    domTable;
  var htmlTable= '<table id="resultTable" class="display" width="100%"></table>';  
function onSparqlQuery(event) {
    event.preventDefault();
    var myData;
    url = "<%=basePath%>result?query=";
    var querytext = document.getElementById("query").value;
    var endpointtext=document.getElementById("endpoints").value;
    if(querytext.length===0 || endpointtext.length===0){
        alert("No blank!");
        return false;
    }
   alert("test:"+querytext);
  var myurl = url + encodeURIComponent(querytext)+"&endpoints="+encodeURIComponent(endpointtext);
  alert(myurl);
    $.ajax({
        type: 'GET',
        url: myurl,
        dataType: 'text',
        success: function (data) {
           renderData(data);
        },
        error: function (data) {
            alert("error: ", data);
        }
    });
}
function renderData(data) {
  $("#linkResult").onclick = function(event) {
        event.preventDefault();
    };
    if ($.fn.DataTable.fnIsDataTable(domTable)) {
        dataTable.fnDestroy(true);
        $('#query-result').append(htmlTable);
    } 
    var jsonData = JSON.parse(data);
    var varName = jsonData.head.vars;
    var arrResult = [];

    var colName = [];
    for (var j = 0; j < varName.length; j++) {
        var temp = varName[j];
        var tempMap = {};
        tempMap['title'] = temp;
        colName.push(tempMap);
    }
    console.log(colName);
    for (var i = 0; i < jsonData.results.bindings.length; i++) {
        var result = jsonData.results.bindings[i];
        var row = [];
        for (var j = 0; j < varName.length; j++) {
            var temp = varName[j];
            row.push(result[temp].value);
        }
        arrResult.push(row);

    }
    console.log(arrResult);   
     dataTable =  $('#resultTable').dataTable({
            data: arrResult,
            columns: colName
        });
    domTable = document.getElementById('resultTable');
    
var blob = new Blob([data], {type: "application/json"});
var myURL = window.URL || window.webkitURL;
var url  = myURL.createObjectURL(blob);

//var a = document.createElement('a');
var myLink=document.getElementById('resultLink');
myLink.download    = "result.json";
myLink.href        = url;

 $("#linkResult").onclick = function(event) {
        return true;
    };
/*$("#resultLink").each(function() {
    this.attr("href", this.data("href")).removeAttr("disabled");
});*/
}

            var sparqlMirror = CodeMirror.fromTextArea(document.getElementById("query"), {
                lineNumbers: true,
                mode: "sparql"    
            });                  
    
            sparqlMirror.on('change', function () {
                     sparqlMirror.save();
                });         
                   console.log("example:" + exampleQueries.lenth);         
                for (var x = 0; x < exampleQueries.length; x++) {
                    var tr = $('<tr></tr>');
                    var td1 = $('<td><a></a></td>');
                    var a = $('<a></a>');
                    a.attr('id', x);
                    a.text(x);
                    td1.append(a);
                    $(a).click(function () {
                        _setTextAreQuery(this)
                    });
                    var td2 = $('<td></td>');
                    td2.text(exampleQueries[x].description);

                    tr.append(td1);
                    tr.append(td2);
                    $('#exampleQueryTable').append(tr);
                }
            
            function _setTextAreQuery(anchor) {
                sparqlMirror.setValue(exampleQueries[anchor.id].query);
            }    
        </script>
        <div id="footer" class="frame">
            <center>
                Contact: {wu,atsuko,jdkim} (at) dbcls.rois.ac.jp.
            </center>
        </div>
    </body>
</html>
